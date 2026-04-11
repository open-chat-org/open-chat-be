import { Controller, Get, Header, Query, Req, Res } from '@nestjs/common';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { create_p2p_graph_html } from './peer_network_graph.template';
import { PeerNetworkService } from './peer_network.service';
import {
  NetworkTraceSeverity,
  NetworkTraceSource,
} from '../network_trace/types/network_trace.types';

@Controller('p2p')
export class PeerNetworkController {
  constructor(
    private readonly network_trace_service: NetworkTraceService,
    private readonly peer_network_service: PeerNetworkService,
  ) {}

  @Get('status')
  get_status() {
    return this.peer_network_service.get_status();
  }

  @Get('topology')
  get_topology() {
    return this.peer_network_service.get_topology();
  }

  @Get('candidates')
  async get_candidates(
    @Query('limit') limit?: string,
    @Query('max_report_age_sec') max_report_age_sec?: string,
  ) {
    const parsed_limit =
      typeof limit === 'string' && Number.isFinite(Number(limit))
        ? Number(limit)
        : undefined;
    const parsed_max_report_age_sec =
      typeof max_report_age_sec === 'string' &&
      Number.isFinite(Number(max_report_age_sec))
        ? Number(max_report_age_sec)
        : undefined;

    return this.peer_network_service.get_candidates(
      parsed_limit,
      parsed_max_report_age_sec,
    );
  }

  @Get('trace')
  get_trace(
    @Query('event_type') event_type?: string,
    @Query('limit') limit?: string,
    @Query('message_id') message_id?: string,
    @Query('peer_id') peer_id?: string,
    @Query('severity') severity?: string,
    @Query('source') source?: string,
  ) {
    const parsed_limit =
      typeof limit === 'string' && Number.isFinite(Number(limit))
        ? Number(limit)
        : undefined;

    return this.network_trace_service.get_recent_events({
      event_type,
      limit: parsed_limit,
      message_id,
      peer_id,
      severity: this.parse_trace_severity(severity),
      source: this.parse_trace_source(source),
    });
  }

  @Get('trace/stream')
  stream_trace(@Req() request: any, @Res() response: any) {
    response.setHeader('Cache-Control', 'no-cache, no-transform');
    response.setHeader('Connection', 'keep-alive');
    response.setHeader('Content-Type', 'text/event-stream');
    response.setHeader('X-Accel-Buffering', 'no');
    response.flushHeaders?.();

    const write_event = (name: string, data: unknown) => {
      response.write(`event: ${name}\n`);
      response.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    write_event('ready', {
      ok: true,
      timestamp: new Date().toISOString(),
    });

    const unsubscribe = this.network_trace_service.subscribe((event) => {
      write_event('trace', event);
    });
    const heartbeat_timer = setInterval(() => {
      write_event('heartbeat', {
        timestamp: new Date().toISOString(),
      });
    }, 15_000);

    request.on('close', () => {
      clearInterval(heartbeat_timer);
      unsubscribe();
      response.end();
    });
  }

  @Get('graph')
  @Header('Content-Type', 'text/html; charset=utf-8')
  get_graph() {
    return create_p2p_graph_html();
  }

  private parse_trace_severity(
    severity?: string,
  ): NetworkTraceSeverity | undefined {
    if (severity === 'error' || severity === 'info' || severity === 'warn') {
      return severity;
    }

    return undefined;
  }

  private parse_trace_source(source?: string): NetworkTraceSource | undefined {
    if (
      source === 'direct_message' ||
      source === 'p2p' ||
      source === 'realtime'
    ) {
      return source;
    }

    return undefined;
  }
}
