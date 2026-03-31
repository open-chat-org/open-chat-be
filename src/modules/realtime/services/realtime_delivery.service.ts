import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { get_realtime_config } from '../../../config/realtime.config';
import {
  DeliveryAckPayload,
  PendingEnvelopeRecord,
  ReliableEnvelope,
  ReliableEnvelopeInput,
} from '../types/realtime.types';
import { create_session_pending_key } from '../utils/realtime_keys.util';
import { RealtimeConnectionService } from './realtime_connection.service';
import { RealtimeRedisService } from './realtime_redis.service';
import { RealtimeSessionService } from './realtime_session.service';
import { randomUUID } from 'node:crypto';

@Injectable()
export class RealtimeDeliveryService implements OnModuleDestroy {
  private readonly realtime_config = get_realtime_config();
  private readonly retry_timers = new Map<string, NodeJS.Timeout>();

  constructor(
    private readonly realtime_connection_service: RealtimeConnectionService,
    private readonly realtime_redis_service: RealtimeRedisService,
    private readonly realtime_session_service: RealtimeSessionService,
  ) {}

  create_envelope<Payload>(
    input: ReliableEnvelopeInput<Payload>,
  ): ReliableEnvelope<Payload> {
    return {
      ack_timeout_ms: input.ack_timeout_ms ?? this.realtime_config.ack_timeout_ms,
      attempt: 0,
      event_id: input.event_id ?? randomUUID(),
      payload: input.payload,
      requires_ack: input.requires_ack ?? true,
      room_id: input.room_id,
      sent_at: new Date().toISOString(),
      type: input.type,
    };
  }

  async deliver_to_session(session_id: string, envelope: ReliableEnvelope) {
    const client = this.realtime_connection_service.get_client_by_session_id(
      session_id,
    );

    if (!client) {
      return;
    }

    if (envelope.requires_ack) {
      await this.store_pending_envelope(session_id, envelope);
      this.schedule_retry(session_id, envelope.event_id, envelope.attempt);
    }

    this.realtime_connection_service.send_json(client, envelope);
  }

  async acknowledge(session_id: string, payload: DeliveryAckPayload) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const deleted_count = await command_client.hdel(
      create_session_pending_key(session_id),
      payload.event_id,
    );

    this.clear_retry_timer(session_id, payload.event_id);

    return deleted_count > 0;
  }

  async replay_pending_for_session(session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const pending_records = await command_client.hgetall(
      create_session_pending_key(session_id),
    );
    const envelopes = Object.values(pending_records)
      .map((raw_value) => JSON.parse(raw_value) as PendingEnvelopeRecord)
      .sort((left, right) => left.sent_at.localeCompare(right.sent_at));
    const client = this.realtime_connection_service.get_client_by_session_id(
      session_id,
    );

    if (!client) {
      return;
    }

    for (const envelope of envelopes) {
      this.realtime_connection_service.send_json(client, envelope);
      this.schedule_retry(session_id, envelope.event_id, envelope.attempt);
    }
  }

  async touch_pending_ttl(session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.pexpire(
      create_session_pending_key(session_id),
      this.realtime_session_service.get_session_ttl_ms(),
    );
  }

  onModuleDestroy() {
    for (const timer of this.retry_timers.values()) {
      clearTimeout(timer);
    }
  }

  private async store_pending_envelope(
    session_id: string,
    envelope: PendingEnvelopeRecord,
  ) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client
      .multi()
      .hset(
        create_session_pending_key(session_id),
        envelope.event_id,
        JSON.stringify(envelope),
      )
      .pexpire(
        create_session_pending_key(session_id),
        this.realtime_session_service.get_session_ttl_ms(),
      )
      .exec();
  }

  private schedule_retry(session_id: string, event_id: string, attempt: number) {
    this.clear_retry_timer(session_id, event_id);

    const backoff_ms =
      this.realtime_config.retry_backoff_ms[
        Math.min(attempt, this.realtime_config.retry_backoff_ms.length - 1)
      ];
    const timer = setTimeout(() => {
      void this.handle_retry_timeout(session_id, event_id);
    }, backoff_ms);

    this.retry_timers.set(this.create_timer_key(session_id, event_id), timer);
  }

  private clear_retry_timer(session_id: string, event_id: string) {
    const timer_key = this.create_timer_key(session_id, event_id);
    const timer = this.retry_timers.get(timer_key);

    if (!timer) {
      return;
    }

    clearTimeout(timer);
    this.retry_timers.delete(timer_key);
  }

  private async handle_retry_timeout(session_id: string, event_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const raw_envelope = await command_client.hget(
      create_session_pending_key(session_id),
      event_id,
    );

    if (!raw_envelope) {
      this.clear_retry_timer(session_id, event_id);
      return;
    }

    const envelope = JSON.parse(raw_envelope) as PendingEnvelopeRecord;

    if (envelope.attempt >= this.realtime_config.max_retries) {
      await command_client.hdel(create_session_pending_key(session_id), event_id);
      this.clear_retry_timer(session_id, event_id);
      this.emit_retry_exhausted(session_id, event_id);
      return;
    }

    const retried_envelope: PendingEnvelopeRecord = {
      ...envelope,
      attempt: envelope.attempt + 1,
    };

    await this.store_pending_envelope(session_id, retried_envelope);

    const client = this.realtime_connection_service.get_client_by_session_id(
      session_id,
    );

    if (client) {
      this.realtime_connection_service.send_json(client, retried_envelope);
    }

    this.schedule_retry(session_id, event_id, retried_envelope.attempt);
  }

  private emit_retry_exhausted(session_id: string, event_id: string) {
    const client = this.realtime_connection_service.get_client_by_session_id(
      session_id,
    );

    if (!client) {
      return;
    }

    this.realtime_connection_service.send_json(client, {
      data: {
        event_id,
        session_id,
      },
      type: 'system.retry_exhausted',
    });
  }

  private create_timer_key(session_id: string, event_id: string) {
    return `${session_id}:${event_id}`;
  }
}
