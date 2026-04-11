import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { get_p2p_config } from '../../config/p2p.config';
import {
  GetRecentTraceEventsInput,
  NetworkTraceEvent,
  RecordNetworkTraceInput,
} from './types/network_trace.types';

type TraceSubscriber = (event: NetworkTraceEvent) => void;

@Injectable()
export class NetworkTraceService {
  private readonly logger = new Logger(NetworkTraceService.name);
  private readonly p2p_config = get_p2p_config();
  private readonly trace_buffer: NetworkTraceEvent[] = [];
  private readonly trace_subscribers = new Set<TraceSubscriber>();

  record_event(input: RecordNetworkTraceInput): NetworkTraceEvent {
    const event: NetworkTraceEvent = {
      ...input,
      details: this.normalize_trace_details(input.details),
      id: randomUUID(),
      timestamp: new Date().toISOString(),
    };

    this.trace_buffer.push(event);

    if (this.trace_buffer.length > this.p2p_config.trace_buffer_size) {
      this.trace_buffer.splice(
        0,
        this.trace_buffer.length - this.p2p_config.trace_buffer_size,
      );
    }

    for (const subscriber of this.trace_subscribers) {
      try {
        subscriber(event);
      } catch (error) {
        this.logger.warn(
          `Trace stream subscriber failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    return event;
  }

  get_recent_events(input: GetRecentTraceEventsInput = {}) {
    const limit = Math.max(1, Math.min(input.limit ?? 200, 5000));
    const filtered_events = this.trace_buffer.filter((event) => {
      if (input.source && event.source !== input.source) {
        return false;
      }

      if (input.severity && event.severity !== input.severity) {
        return false;
      }

      if (input.event_type && event.event_type !== input.event_type) {
        return false;
      }

      if (input.peer_id && event.peer_id !== input.peer_id) {
        return false;
      }

      if (input.message_id && event.message_id !== input.message_id) {
        return false;
      }

      return true;
    });

    return filtered_events
      .slice(Math.max(filtered_events.length - limit, 0))
      .reverse();
  }

  subscribe(subscriber: TraceSubscriber) {
    this.trace_subscribers.add(subscriber);

    return () => {
      this.trace_subscribers.delete(subscriber);
    };
  }

  private normalize_trace_details(details: unknown) {
    if (details === undefined) {
      return undefined;
    }

    try {
      const json = JSON.stringify(details);

      if (json === undefined) {
        return undefined;
      }

      const size_bytes = Buffer.byteLength(json, 'utf8');

      if (size_bytes <= this.p2p_config.trace_max_payload_bytes) {
        return JSON.parse(json);
      }

      const preview = json.slice(0, this.p2p_config.trace_max_payload_bytes);

      return {
        original_size_bytes: size_bytes,
        preview,
        truncated: true,
      };
    } catch (error) {
      return {
        reason: error instanceof Error ? error.message : String(error),
        serialization_failed: true,
      };
    }
  }
}

