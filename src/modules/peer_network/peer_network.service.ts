import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { ServerIdentityService } from '../server_identity/server_identity.service';
import { get_p2p_config } from '../../config/p2p.config';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import {
  PEER_HELLO_PROTOCOL,
  PEER_SERVER_SCORE_GOSSIP_PROTOCOL,
} from './constants/peer_protocol.constants';
import {
  ServerCandidate,
  ServerScoreReportPayload,
  ServerScoreReportValidationResult,
} from './types/server_score.types';
import {
  create_server_score_signature,
  verify_server_score_signature,
} from './utils/server_score_signature.util';

type ConnectedPeerSnapshot = {
  peer_id: string;
  remote_address: string | null;
};

type StreamChunk = Uint8Array | { subarray: (...args: number[]) => Uint8Array };

type LocalPeerObservation = {
  connect_count: number;
  dial_attempt_count: number;
  dial_failure_count: number;
  disconnect_count: number;
  is_currently_connected: boolean;
  last_event_at_ms: number;
  last_successful_connect_at_ms: number | null;
};

type CandidateAggregateSnapshot = {
  last_observed_at_ms: number;
  mean_score: number;
  report_count: number;
  target_peer_id: string;
};

function normalize_chunk(chunk: StreamChunk): Uint8Array {
  return chunk instanceof Uint8Array ? chunk : chunk.subarray();
}

function create_single_chunk_source(payload: string) {
  return (async function* () {
    yield new TextEncoder().encode(payload);
  })();
}

@Injectable()
export class PeerNetworkService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PeerNetworkService.name);
  private readonly p2p_config = get_p2p_config();
  private readonly local_peer_observations = new Map<
    string,
    LocalPeerObservation
  >();
  private libp2p_node: any = null;
  private score_gossip_timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly network_trace_service: NetworkTraceService,
    private readonly prisma_service: PrismaService,
    private readonly server_identity_service: ServerIdentityService,
  ) {}

  async onModuleInit() {
    if (!this.p2p_config.enabled) {
      this.logger.log('P2P is disabled');
      this.trace_event('p2p.disabled', 'info', {
        reason: 'P2P_ENABLED is false',
      });
      return;
    }

    const [
      { createLibp2p },
      { tcp },
      { quic },
      { noise },
      { yamux },
      { kadDHT },
      { ping },
      { identify },
      { multiaddr },
    ] = await Promise.all([
      import('libp2p'),
      import('@libp2p/tcp'),
      import('@chainsafe/libp2p-quic'),
      import('@chainsafe/libp2p-noise'),
      import('@libp2p/yamux'),
      import('@libp2p/kad-dht'),
      import('@libp2p/ping'),
      import('@libp2p/identify'),
      import('@multiformats/multiaddr'),
    ]);

    this.libp2p_node = await createLibp2p({
      addresses: {
        listen: [this.p2p_config.listen],
      },
      transports: [tcp(), quic()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      connectionManager: {
        maxConnections: Math.max(32, this.p2p_config.target_peers * 2),
        maxParallelDials: Math.max(25, this.p2p_config.target_peers),
        maxIncomingPendingConnections: 20,
      },
      services: {
        identify: identify(),
        ping: ping(),
        dht: kadDHT({
          kBucketSize: this.p2p_config.k_bucket_size,
          clientMode: false,
          alpha: this.p2p_config.alpha,
        }),
      },
    });

    this.logger.log(`P2P node started on ${this.p2p_config.listen}`);
    this.trace_event('p2p.node_started', 'info', {
      listen: this.p2p_config.listen,
    });

    this.register_protocol_handlers();
    this.log_node_identity();
    await this.sync_local_server_node_state(true);

    for (const bootstrap_address of this.p2p_config.bootstrap) {
      const normalized_address =
        this.normalize_bootstrap_address(bootstrap_address);
      const peer_id = this.extract_peer_id_from_multiaddr(normalized_address);
      this.note_dial_attempt(peer_id);

      this.trace_event(
        'p2p.bootstrap_dial_attempt',
        'info',
        {
          address: normalized_address,
        },
        peer_id,
      );

      try {
        await this.libp2p_node.dial(multiaddr(normalized_address));
        this.logger.log(`Dialed bootstrap peer: ${normalized_address}`);
        this.note_dial_success(peer_id);
        this.trace_event(
          'p2p.bootstrap_dial_succeeded',
          'info',
          {
            address: normalized_address,
          },
          peer_id,
        );
      } catch (error) {
        const error_message =
          error instanceof Error ? error.message : String(error);
        this.note_dial_failure(peer_id);
        this.logger.warn(
          `Bootstrap dial failed: ${bootstrap_address} (${error_message})`,
        );
        this.trace_event(
          'p2p.bootstrap_dial_failed',
          'warn',
          {
            address: normalized_address,
            error: error_message,
          },
          peer_id,
        );
      }
    }

    await this.run_server_score_cycle();
    this.score_gossip_timer = setInterval(() => {
      void this.run_server_score_cycle();
    }, this.p2p_config.score_gossip_interval_ms);
  }

  async onModuleDestroy() {
    if (this.score_gossip_timer) {
      clearInterval(this.score_gossip_timer);
    }

    await this.sync_local_server_node_state(false);

    if (this.libp2p_node == null) {
      return;
    }

    await this.libp2p_node.stop();
    this.logger.log('P2P node stopped');
    this.trace_event('p2p.node_stopped', 'info');
  }

  get_status() {
    const topology = this.get_topology();

    if (!topology.enabled) {
      return {
        connected_peers: [],
        connected_peers_count: 0,
        enabled: false,
        generated_at: topology.generated_at,
        listen_addresses: [],
        local_peer_id: null,
      };
    }

    return {
      connected_peers: topology.connected_peers,
      connected_peers_count: topology.connected_peers.length,
      enabled: true,
      generated_at: topology.generated_at,
      listen_addresses: topology.local_node.listen_addresses,
      local_peer_id: topology.local_node.peer_id,
    };
  }

  async get_candidates(limit?: number, max_report_age_sec?: number) {
    const now = new Date();
    const normalized_limit = this.normalize_limit(limit);
    const normalized_max_report_age_seconds =
      this.normalize_max_report_age_seconds(max_report_age_sec);
    const aggregates = await this.compute_candidate_aggregates(
      normalized_max_report_age_seconds,
      now,
    );
    const selected_aggregates = aggregates.slice(0, normalized_limit);
    const node_records = await this.prisma_service.serverNode.findMany({
      where: {
        peer_id: {
          in: selected_aggregates.map((aggregate) => aggregate.target_peer_id),
        },
      },
    });
    const node_record_map = new Map(
      node_records.map((record) => [record.peer_id, record]),
    );
    const candidates: ServerCandidate[] = selected_aggregates.map(
      (aggregate) => {
        const node_record = node_record_map.get(aggregate.target_peer_id);

        return {
          is_active: node_record?.is_active ?? false,
          last_seen_at:
            node_record?.last_seen_at.toISOString() ??
            new Date(aggregate.last_observed_at_ms).toISOString(),
          listen_addresses: this.parse_listen_addresses(
            node_record?.listen_addresses,
          ),
          mean_score: Number(aggregate.mean_score.toFixed(2)),
          peer_id: aggregate.target_peer_id,
          report_count: aggregate.report_count,
          server_public_key: node_record?.server_public_key ?? null,
        };
      },
    );

    this.trace_event('p2p.score_candidates_read', 'info', {
      candidate_count: candidates.length,
      limit: normalized_limit,
      max_report_age_sec: normalized_max_report_age_seconds,
    });

    return {
      candidates,
      generated_at: now.toISOString(),
      limit: normalized_limit,
      max_report_age_sec: normalized_max_report_age_seconds,
    };
  }

  get_topology() {
    const generated_at = new Date().toISOString();

    if (this.libp2p_node == null) {
      return {
        connected_peers: [] as ConnectedPeerSnapshot[],
        enabled: false,
        generated_at,
        links: [] as Array<{ source: string; target: string; type: string }>,
        local_node: {
          listen_addresses: [] as string[],
          peer_id: null as string | null,
        },
        nodes: [] as Array<{ id: string; label: string; type: string }>,
      };
    }

    const local_peer_id = this.get_local_peer_id();
    const connected_peers = this.get_connected_peers();
    const listen_addresses = this.get_local_listen_addresses();

    return {
      connected_peers,
      enabled: true,
      generated_at,
      links: connected_peers.map((peer) => ({
        source: local_peer_id,
        target: peer.peer_id,
        type: 'p2p',
      })),
      local_node: {
        listen_addresses,
        peer_id: local_peer_id,
      },
      nodes: [
        {
          id: local_peer_id,
          label: 'This server',
          type: 'local',
        },
        ...connected_peers.map((peer) => ({
          id: peer.peer_id,
          label: peer.peer_id.slice(0, 12),
          type: 'peer',
        })),
      ],
    };
  }

  private register_protocol_handlers() {
    if (this.libp2p_node == null) {
      return;
    }

    this.libp2p_node.handle(
      PEER_HELLO_PROTOCOL,
      async ({ connection, stream }: any) => {
        const remote_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        this.logger.log(`Received hello from peer: ${remote_peer_id}`);
        this.trace_event(
          'p2p.hello_received',
          'info',
          undefined,
          remote_peer_id,
        );
        await this.touch_peer_node(remote_peer_id, {
          is_active: this.is_peer_directly_connected(remote_peer_id),
        });

        if (stream?.sink != null) {
          await stream.sink(create_single_chunk_source('hello-ack'));
          this.trace_event(
            'p2p.hello_ack_sent',
            'info',
            undefined,
            remote_peer_id,
          );
        }
      },
    );

    this.libp2p_node.handle(
      PEER_SERVER_SCORE_GOSSIP_PROTOCOL,
      async ({ connection, stream }: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);

        if (!payload_text.trim()) {
          this.trace_event(
            'p2p.score_gossip_rejected',
            'warn',
            {
              reason: 'Empty score gossip payload.',
            },
            sender_peer_id,
          );
          return;
        }

        let parsed_payload: unknown;

        try {
          parsed_payload = JSON.parse(payload_text);
        } catch (error) {
          this.trace_event(
            'p2p.score_gossip_rejected',
            'warn',
            {
              reason: 'Invalid score gossip JSON payload.',
              error: error instanceof Error ? error.message : String(error),
            },
            sender_peer_id,
          );
          return;
        }

        const report_values = Array.isArray(parsed_payload)
          ? parsed_payload
          : [parsed_payload];

        for (const report_value of report_values) {
          const validation_result =
            this.validate_server_score_report_payload(report_value);

          if (!validation_result.ok || !validation_result.payload) {
            this.trace_event(
              'p2p.score_gossip_rejected',
              'warn',
              {
                reason:
                  validation_result.error ??
                  'Score gossip payload failed validation.',
              },
              sender_peer_id,
            );
            continue;
          }

          await this.handle_incoming_server_score_report(
            validation_result.payload,
            sender_peer_id,
          );
        }
      },
    );

    this.libp2p_node.addEventListener?.('peer:connect', (event: any) => {
      const connection = event?.detail;
      const peer_id =
        connection?.remotePeer?.toString?.() ??
        connection?.toString?.() ??
        'unknown_peer';
      const remote_address = connection?.remoteAddr?.toString?.() ?? null;
      this.logger.log(`Peer connected: ${peer_id}`);
      this.trace_event('p2p.peer_connected', 'info', undefined, peer_id);
      this.note_peer_connected(peer_id);
      void this.touch_peer_node(peer_id, {
        is_active: true,
        listen_addresses: remote_address ? [remote_address] : undefined,
      });
    });

    this.libp2p_node.addEventListener?.('peer:disconnect', (event: any) => {
      const connection = event?.detail;
      const peer_id =
        connection?.remotePeer?.toString?.() ??
        connection?.toString?.() ??
        'unknown_peer';
      this.logger.warn(`Peer disconnected: ${peer_id}`);
      this.trace_event('p2p.peer_disconnected', 'warn', undefined, peer_id);
      this.note_peer_disconnected(peer_id);
      void this.touch_peer_node(peer_id, {
        is_active: false,
      });
    });
  }

  private log_node_identity() {
    if (this.libp2p_node == null) {
      return;
    }

    const peer_id = this.get_local_peer_id();
    const listen_addresses = this.get_local_listen_addresses();

    this.logger.log(`P2P node identity: ${peer_id}`);
    for (const address of listen_addresses) {
      this.logger.log(`P2P listen address: ${address}`);
    }

    this.trace_event(
      'p2p.identity_announced',
      'info',
      {
        listen_addresses,
      },
      peer_id,
    );
  }

  private get_local_peer_id() {
    return this.libp2p_node?.peerId?.toString?.() ?? 'unknown_peer';
  }

  private get_local_listen_addresses() {
    const local_peer_id = this.get_local_peer_id();

    return (
      this.libp2p_node?.getMultiaddrs?.()?.map((address: any) => {
        const address_text = address.toString();
        const peer_suffix = `/p2p/${local_peer_id}`;
        return address_text.endsWith(peer_suffix)
          ? address_text
          : `${address_text}${peer_suffix}`;
      }) ?? []
    );
  }

  private get_connected_peers(): ConnectedPeerSnapshot[] {
    const peer_map = new Map<string, ConnectedPeerSnapshot>();
    const connections = this.libp2p_node?.getConnections?.() ?? [];

    for (const connection of connections) {
      const peer_id = connection?.remotePeer?.toString?.();
      if (!peer_id) {
        continue;
      }

      const remote_address = connection?.remoteAddr?.toString?.() ?? null;
      const existing_peer = peer_map.get(peer_id);

      if (!existing_peer) {
        peer_map.set(peer_id, {
          peer_id,
          remote_address,
        });
        continue;
      }

      if (!existing_peer.remote_address && remote_address) {
        existing_peer.remote_address = remote_address;
      }
    }

    return Array.from(peer_map.values()).sort((a, b) =>
      a.peer_id.localeCompare(b.peer_id),
    );
  }

  private async run_server_score_cycle() {
    if (this.libp2p_node == null) {
      return;
    }

    try {
      await this.sync_local_server_node_state(true);

      const local_reports = await this.generate_local_server_score_reports();
      const reports_to_gossip =
        await this.collect_reports_for_gossip(local_reports);

      await this.gossip_reports_to_connected_peers(reports_to_gossip);
      await this.delete_expired_server_score_reports();
    } catch (error) {
      this.trace_event('p2p.score_cycle_failed', 'warn', {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private async generate_local_server_score_reports() {
    if (this.libp2p_node == null) {
      return [] as ServerScoreReportPayload[];
    }

    const local_peer_id = this.get_local_peer_id();
    const connected_peer_ids = this.get_connected_peers().map(
      (peer) => peer.peer_id,
    );
    const candidate_peer_ids = new Set(connected_peer_ids);
    const max_observation_age_ms =
      this.p2p_config.score_default_max_report_age_seconds * 2 * 1000;
    const now_ms = Date.now();

    for (const [
      peer_id,
      observation,
    ] of this.local_peer_observations.entries()) {
      if (peer_id === local_peer_id) {
        continue;
      }

      if (now_ms - observation.last_event_at_ms <= max_observation_age_ms) {
        candidate_peer_ids.add(peer_id);
      }
    }

    if (candidate_peer_ids.size === 0) {
      return [] as ServerScoreReportPayload[];
    }

    const identity = await this.server_identity_service.get_public_key();
    const reports: ServerScoreReportPayload[] = [];

    for (const target_peer_id of candidate_peer_ids) {
      const score = this.compute_local_score_for_peer(target_peer_id);
      const observed_at = new Date();
      const expires_at = new Date(
        observed_at.getTime() + this.p2p_config.score_report_ttl_seconds * 1000,
      );
      const { payload_hash, signature } = await create_server_score_signature(
        {
          expires_at: expires_at.toISOString(),
          observed_at: observed_at.toISOString(),
          reporter_peer_id: local_peer_id,
          reporter_server_public_key: identity.public_key,
          score,
          target_peer_id,
        },
        async (message) => this.server_identity_service.sign_message(message),
      );
      const report: ServerScoreReportPayload = {
        expires_at: expires_at.toISOString(),
        observed_at: observed_at.toISOString(),
        payload_hash,
        reporter_peer_id: local_peer_id,
        reporter_server_public_key: identity.public_key,
        score,
        signature,
        target_peer_id,
        type: 'server.score.report',
      };

      await this.persist_server_score_report(report, 'local_observation');
      this.trace_event(
        'p2p.score_local_report_created',
        'info',
        {
          score,
          target_peer_id,
        },
        target_peer_id,
      );
      reports.push(report);
    }

    return reports;
  }

  private async collect_reports_for_gossip(
    local_reports: ServerScoreReportPayload[],
  ) {
    const now = new Date();
    const cutoff_date = new Date(
      now.getTime() -
        this.p2p_config.score_default_max_report_age_seconds * 1000,
    );
    const fresh_reports = await this.prisma_service.serverScoreReport.findMany({
      where: {
        expires_at: {
          gt: now,
        },
        observed_at: {
          gte: cutoff_date,
        },
      },
      orderBy: [
        {
          observed_at: 'desc',
        },
        {
          createdAt: 'desc',
        },
      ],
      take: 500,
    });
    const report_map = new Map<string, ServerScoreReportPayload>();

    for (const report of local_reports) {
      report_map.set(
        `${report.reporter_peer_id}:${report.target_peer_id}`,
        report,
      );
    }

    for (const report of fresh_reports) {
      const map_key = `${report.reporter_peer_id}:${report.target_peer_id}`;

      if (!report_map.has(map_key)) {
        report_map.set(map_key, {
          expires_at: report.expires_at.toISOString(),
          observed_at: report.observed_at.toISOString(),
          payload_hash: report.payload_hash,
          reporter_peer_id: report.reporter_peer_id,
          reporter_server_public_key: report.reporter_server_public_key,
          score: report.score,
          signature: report.signature,
          target_peer_id: report.target_peer_id,
          type: 'server.score.report',
        });
      }
    }

    return Array.from(report_map.values()).slice(0, 120);
  }

  private async gossip_reports_to_connected_peers(
    reports: ServerScoreReportPayload[],
  ) {
    if (reports.length === 0) {
      return;
    }

    const connected_peers = this.get_connected_peers();

    if (connected_peers.length === 0) {
      return;
    }

    await Promise.all(
      connected_peers.map(async (peer) =>
        this.send_server_score_reports_to_peer(peer.peer_id, reports),
      ),
    );
  }

  private async send_server_score_reports_to_peer(
    peer_id: string,
    reports: ServerScoreReportPayload[],
  ) {
    if (this.libp2p_node == null) {
      return;
    }

    const peer_connection = this.get_connected_peer_connection(peer_id);

    if (!peer_connection?.remotePeer) {
      return;
    }

    try {
      const { stream } = await this.libp2p_node.dialProtocol(
        peer_connection.remotePeer,
        PEER_SERVER_SCORE_GOSSIP_PROTOCOL,
      );

      await stream.sink(create_single_chunk_source(JSON.stringify(reports)));
      this.trace_event(
        'p2p.score_gossip_sent',
        'info',
        {
          report_count: reports.length,
        },
        peer_id,
      );
    } catch (error) {
      const error_message =
        error instanceof Error ? error.message : String(error);
      this.trace_event(
        'p2p.score_gossip_send_failed',
        'warn',
        {
          error: error_message,
          report_count: reports.length,
        },
        peer_id,
      );
    }
  }

  private async handle_incoming_server_score_report(
    report: ServerScoreReportPayload,
    sender_peer_id: string,
  ) {
    if (!this.is_peer_directly_connected(sender_peer_id)) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Score gossip sender is not currently a direct peer.',
          sender_peer_id,
        },
        sender_peer_id,
      );
      return;
    }

    const now_ms = Date.now();
    const observed_at_ms = new Date(report.observed_at).getTime();
    const expires_at_ms = new Date(report.expires_at).getTime();

    if (Number.isNaN(observed_at_ms) || Number.isNaN(expires_at_ms)) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Score gossip report contains invalid timestamps.',
        },
        sender_peer_id,
      );
      return;
    }

    if (expires_at_ms <= now_ms) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Score gossip report is already expired.',
        },
        sender_peer_id,
      );
      return;
    }

    const max_age_ms =
      this.p2p_config.score_default_max_report_age_seconds * 1000;

    if (now_ms - observed_at_ms > max_age_ms) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Score gossip report is stale.',
          observed_at: report.observed_at,
        },
        sender_peer_id,
      );
      return;
    }

    const existing_reporter_node = await this.prisma_service.serverNode.findUnique({
      where: {
        peer_id: report.reporter_peer_id,
      },
      select: {
        server_public_key: true,
      },
    });

    if (
      existing_reporter_node?.server_public_key &&
      this.normalize_hex(existing_reporter_node.server_public_key) !==
        this.normalize_hex(report.reporter_server_public_key)
    ) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason:
            'Score gossip reporter public key does not match known reporter identity.',
          reporter_peer_id: report.reporter_peer_id,
          sender_peer_id,
        },
        sender_peer_id,
      );
      return;
    }

    const has_valid_signature = await verify_server_score_signature(report);

    if (!has_valid_signature) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Score gossip signature verification failed.',
          reporter_peer_id: report.reporter_peer_id,
          sender_peer_id,
        },
        sender_peer_id,
      );
      return;
    }

    await this.persist_server_score_report(report, 'gossip');
    await this.touch_peer_node(report.reporter_peer_id, {
      is_active: this.is_peer_directly_connected(report.reporter_peer_id),
      last_announce_at: new Date(report.observed_at),
      server_public_key: report.reporter_server_public_key,
    });
    this.trace_event(
      'p2p.score_gossip_received',
      'info',
      {
        reporter_peer_id: report.reporter_peer_id,
        score: report.score,
        sender_peer_id,
        target_peer_id: report.target_peer_id,
      },
      sender_peer_id,
    );
  }

  private validate_server_score_report_payload(
    value: unknown,
  ): ServerScoreReportValidationResult {
    if (typeof value !== 'object' || value == null) {
      return {
        error: 'Score gossip payload must be an object.',
        ok: false,
      };
    }

    const candidate = value as Record<string, unknown>;

    if (candidate.type !== 'server.score.report') {
      return {
        error: 'Unsupported score gossip payload type.',
        ok: false,
      };
    }

    if (
      typeof candidate.reporter_peer_id !== 'string' ||
      !candidate.reporter_peer_id.trim()
    ) {
      return {
        error: 'reporter_peer_id is required.',
        ok: false,
      };
    }

    if (
      typeof candidate.target_peer_id !== 'string' ||
      !candidate.target_peer_id.trim()
    ) {
      return {
        error: 'target_peer_id is required.',
        ok: false,
      };
    }

    if (
      typeof candidate.reporter_server_public_key !== 'string' ||
      !this.is_hex(candidate.reporter_server_public_key)
    ) {
      return {
        error: 'reporter_server_public_key must be a valid hex string.',
        ok: false,
      };
    }

    if (
      typeof candidate.score !== 'number' ||
      !Number.isFinite(candidate.score)
    ) {
      return {
        error: 'score must be a valid number.',
        ok: false,
      };
    }

    const normalized_score = Math.round(candidate.score);

    if (normalized_score < 0 || normalized_score > 100) {
      return {
        error: 'score must be within the 0..100 range.',
        ok: false,
      };
    }

    if (
      typeof candidate.observed_at !== 'string' ||
      Number.isNaN(new Date(candidate.observed_at).getTime())
    ) {
      return {
        error: 'observed_at must be an ISO timestamp.',
        ok: false,
      };
    }

    if (
      typeof candidate.expires_at !== 'string' ||
      Number.isNaN(new Date(candidate.expires_at).getTime())
    ) {
      return {
        error: 'expires_at must be an ISO timestamp.',
        ok: false,
      };
    }

    if (
      typeof candidate.payload_hash !== 'string' ||
      !this.is_hex(candidate.payload_hash)
    ) {
      return {
        error: 'payload_hash must be a valid hex string.',
        ok: false,
      };
    }

    if (
      typeof candidate.signature !== 'string' ||
      !this.is_hex(candidate.signature)
    ) {
      return {
        error: 'signature must be a valid hex string.',
        ok: false,
      };
    }

    return {
      ok: true,
      payload: {
        expires_at: candidate.expires_at,
        observed_at: candidate.observed_at,
        payload_hash: candidate.payload_hash,
        reporter_peer_id: candidate.reporter_peer_id.trim(),
        reporter_server_public_key: this.normalize_hex(
          candidate.reporter_server_public_key,
        ),
        score: normalized_score,
        signature: this.normalize_hex(candidate.signature),
        target_peer_id: candidate.target_peer_id.trim(),
        type: 'server.score.report',
      },
    };
  }

  private async persist_server_score_report(
    report: ServerScoreReportPayload,
    source: 'gossip' | 'local_observation',
  ) {
    const now = new Date();
    const observed_at = new Date(report.observed_at);
    const expires_at = new Date(report.expires_at);
    const latest_report = await this.prisma_service.serverScoreReport.findFirst(
      {
        where: {
          expires_at: {
            gt: now,
          },
          reporter_peer_id: report.reporter_peer_id,
          target_peer_id: report.target_peer_id,
        },
        orderBy: [
          {
            observed_at: 'desc',
          },
          {
            createdAt: 'desc',
          },
        ],
      },
    );

    if (
      latest_report &&
      latest_report.observed_at.getTime() > observed_at.getTime()
    ) {
      this.trace_event(
        'p2p.score_gossip_rejected',
        'warn',
        {
          reason: 'Older score report ignored due to newer observation.',
          reporter_peer_id: report.reporter_peer_id,
          target_peer_id: report.target_peer_id,
        },
        report.reporter_peer_id,
      );
      return;
    }

    await this.prisma_service.serverScoreReport.upsert({
      where: {
        reporter_peer_id_target_peer_id_observed_at: {
          observed_at,
          reporter_peer_id: report.reporter_peer_id,
          target_peer_id: report.target_peer_id,
        },
      },
      update: {
        expires_at,
        payload_hash: report.payload_hash,
        reporter_server_public_key: report.reporter_server_public_key,
        score: report.score,
        signature: report.signature,
      },
      create: {
        expires_at,
        observed_at,
        payload_hash: report.payload_hash,
        reporter_peer_id: report.reporter_peer_id,
        reporter_server_public_key: report.reporter_server_public_key,
        score: report.score,
        signature: report.signature,
        target_peer_id: report.target_peer_id,
      },
    });

    await this.prisma_service.serverScoreReport.deleteMany({
      where: {
        expires_at: {
          gt: now,
        },
        observed_at: {
          lt: observed_at,
        },
        reporter_peer_id: report.reporter_peer_id,
        target_peer_id: report.target_peer_id,
      },
    });
    await this.recompute_score_aggregate_for_target(report.target_peer_id);

    this.trace_event('p2p.score_report_persisted', 'info', {
      reporter_peer_id: report.reporter_peer_id,
      score: report.score,
      source,
      target_peer_id: report.target_peer_id,
    });
  }

  private async recompute_score_aggregate_for_target(target_peer_id: string) {
    const now = new Date();
    const aggregates = await this.compute_candidate_aggregates(
      this.p2p_config.score_default_max_report_age_seconds,
      now,
      [target_peer_id],
    );
    const aggregate = aggregates[0];

    if (!aggregate) {
      await this.prisma_service.serverScoreAggregate.deleteMany({
        where: {
          target_peer_id,
        },
      });
      this.trace_event('p2p.score_aggregate_deleted', 'info', {
        target_peer_id,
      });
      return;
    }

    await this.prisma_service.serverScoreAggregate.upsert({
      where: {
        target_peer_id,
      },
      update: {
        last_report_at: new Date(aggregate.last_observed_at_ms),
        mean_score: aggregate.mean_score,
        report_count: aggregate.report_count,
        updated_at: now,
      },
      create: {
        last_report_at: new Date(aggregate.last_observed_at_ms),
        mean_score: aggregate.mean_score,
        report_count: aggregate.report_count,
        target_peer_id,
        updated_at: now,
      },
    });
    this.trace_event('p2p.score_aggregate_updated', 'info', {
      mean_score: aggregate.mean_score,
      report_count: aggregate.report_count,
      target_peer_id,
    });
  }

  private async compute_candidate_aggregates(
    max_report_age_seconds: number,
    now: Date,
    target_peer_ids?: string[],
  ) {
    const cutoff_date = new Date(now.getTime() - max_report_age_seconds * 1000);
    const reports = await this.prisma_service.serverScoreReport.findMany({
      where: {
        expires_at: {
          gt: now,
        },
        observed_at: {
          gte: cutoff_date,
        },
        ...(target_peer_ids && target_peer_ids.length > 0
          ? {
              target_peer_id: {
                in: target_peer_ids,
              },
            }
          : {}),
      },
      orderBy: [
        {
          observed_at: 'desc',
        },
        {
          createdAt: 'desc',
        },
      ],
    });
    const latest_report_map = new Map<string, (typeof reports)[number]>();

    for (const report of reports) {
      const map_key = `${report.reporter_peer_id}:${report.target_peer_id}`;

      if (!latest_report_map.has(map_key)) {
        latest_report_map.set(map_key, report);
      }
    }

    const aggregate_map = new Map<
      string,
      {
        last_observed_at_ms: number;
        score_sum: number;
        score_vote_count: number;
      }
    >();

    for (const report of latest_report_map.values()) {
      const existing = aggregate_map.get(report.target_peer_id);
      const observed_at_ms = report.observed_at.getTime();

      if (!existing) {
        aggregate_map.set(report.target_peer_id, {
          last_observed_at_ms: observed_at_ms,
          score_sum: report.score,
          score_vote_count: 1,
        });
        continue;
      }

      existing.score_sum += report.score;
      existing.score_vote_count += 1;
      existing.last_observed_at_ms = Math.max(
        existing.last_observed_at_ms,
        observed_at_ms,
      );
    }

    return Array.from(aggregate_map.entries())
      .map(
        ([target_peer_id, value]): CandidateAggregateSnapshot => ({
          last_observed_at_ms: value.last_observed_at_ms,
          mean_score: value.score_sum / value.score_vote_count,
          report_count: value.score_vote_count,
          target_peer_id,
        }),
      )
      .sort((left, right) => {
        if (right.mean_score !== left.mean_score) {
          return right.mean_score - left.mean_score;
        }

        if (right.report_count !== left.report_count) {
          return right.report_count - left.report_count;
        }

        return right.last_observed_at_ms - left.last_observed_at_ms;
      });
  }

  private async delete_expired_server_score_reports() {
    const delete_result =
      await this.prisma_service.serverScoreReport.deleteMany({
        where: {
          expires_at: {
            lte: new Date(),
          },
        },
      });

    if (delete_result.count > 0) {
      this.trace_event('p2p.score_expired_reports_deleted', 'info', {
        count: delete_result.count,
      });
    }
  }

  private async sync_local_server_node_state(is_active: boolean) {
    if (this.libp2p_node == null) {
      return;
    }

    const local_peer_id = this.get_local_peer_id();
    const listen_addresses = this.get_local_listen_addresses();
    const server_public_key = (
      await this.server_identity_service.get_public_key()
    ).public_key;
    const now = new Date();

    await this.prisma_service.serverNode.upsert({
      where: {
        peer_id: local_peer_id,
      },
      update: {
        is_active,
        last_announce_at: now,
        last_seen_at: now,
        listen_addresses,
        server_public_key,
      },
      create: {
        is_active,
        last_announce_at: now,
        last_seen_at: now,
        listen_addresses,
        peer_id: local_peer_id,
        server_public_key,
      },
    });
  }

  private async touch_peer_node(
    peer_id: string,
    options: {
      is_active?: boolean;
      last_announce_at?: Date;
      listen_addresses?: string[];
      server_public_key?: string;
    },
  ) {
    if (!peer_id || peer_id === 'unknown_peer') {
      return;
    }

    const now = new Date();
    const update_payload: {
      is_active?: boolean;
      last_announce_at?: Date;
      last_seen_at: Date;
      listen_addresses?: string[];
      server_public_key?: string;
    } = {
      last_seen_at: now,
    };

    if (options.is_active !== undefined) {
      update_payload.is_active = options.is_active;
    }

    if (options.last_announce_at !== undefined) {
      update_payload.last_announce_at = options.last_announce_at;
    }

    if (options.listen_addresses !== undefined) {
      update_payload.listen_addresses = options.listen_addresses;
    }

    if (options.server_public_key !== undefined) {
      update_payload.server_public_key = options.server_public_key;
    }

    await this.prisma_service.serverNode.upsert({
      where: {
        peer_id,
      },
      update: update_payload,
      create: {
        is_active: options.is_active ?? false,
        last_announce_at: options.last_announce_at,
        last_seen_at: now,
        listen_addresses: options.listen_addresses,
        peer_id,
        server_public_key: options.server_public_key,
      },
    });
  }

  private note_dial_attempt(peer_id?: string) {
    if (!peer_id) {
      return;
    }

    const observation = this.get_peer_observation(peer_id);
    observation.dial_attempt_count += 1;
    observation.last_event_at_ms = Date.now();
  }

  private note_dial_success(peer_id?: string) {
    if (!peer_id) {
      return;
    }

    const observation = this.get_peer_observation(peer_id);
    observation.connect_count += 1;
    observation.is_currently_connected = true;
    observation.last_event_at_ms = Date.now();
    observation.last_successful_connect_at_ms = Date.now();
  }

  private note_dial_failure(peer_id?: string) {
    if (!peer_id) {
      return;
    }

    const observation = this.get_peer_observation(peer_id);
    observation.dial_failure_count += 1;
    observation.last_event_at_ms = Date.now();
  }

  private note_peer_connected(peer_id: string) {
    if (!peer_id || peer_id === 'unknown_peer') {
      return;
    }

    const observation = this.get_peer_observation(peer_id);
    observation.connect_count += 1;
    observation.is_currently_connected = true;
    observation.last_event_at_ms = Date.now();
    observation.last_successful_connect_at_ms = Date.now();
  }

  private note_peer_disconnected(peer_id: string) {
    if (!peer_id || peer_id === 'unknown_peer') {
      return;
    }

    const observation = this.get_peer_observation(peer_id);
    observation.disconnect_count += 1;
    observation.is_currently_connected = false;
    observation.last_event_at_ms = Date.now();
  }

  private get_peer_observation(peer_id: string) {
    const existing_observation = this.local_peer_observations.get(peer_id);

    if (existing_observation) {
      return existing_observation;
    }

    const created_observation: LocalPeerObservation = {
      connect_count: 0,
      dial_attempt_count: 0,
      dial_failure_count: 0,
      disconnect_count: 0,
      is_currently_connected: false,
      last_event_at_ms: Date.now(),
      last_successful_connect_at_ms: null,
    };

    this.local_peer_observations.set(peer_id, created_observation);

    return created_observation;
  }

  private compute_local_score_for_peer(peer_id: string) {
    const observation = this.local_peer_observations.get(peer_id);

    if (!observation) {
      return 50;
    }

    let score = 70;

    score += Math.min(observation.connect_count * 4, 20);
    score -= Math.min(observation.dial_failure_count * 12, 45);
    score -= Math.min(observation.disconnect_count * 5, 25);

    if (observation.is_currently_connected) {
      score += 10;
    }

    if (observation.last_successful_connect_at_ms != null) {
      const seconds_since_last_success =
        (Date.now() - observation.last_successful_connect_at_ms) / 1000;

      if (seconds_since_last_success > 180) {
        score -= 10;
      }
    }

    return Math.max(0, Math.min(100, Math.round(score)));
  }

  private get_connected_peer_connection(peer_id: string) {
    const connections = this.libp2p_node?.getConnections?.() ?? [];

    return (
      connections.find(
        (connection: any) =>
          connection?.remotePeer?.toString?.() === peer_id &&
          connection?.status !== 'closed',
      ) ?? null
    );
  }

  private is_peer_directly_connected(peer_id: string) {
    return this.get_connected_peer_connection(peer_id) != null;
  }

  private normalize_limit(limit?: number) {
    if (typeof limit !== 'number' || !Number.isFinite(limit)) {
      return 12;
    }

    return Math.max(1, Math.min(100, Math.trunc(limit)));
  }

  private normalize_max_report_age_seconds(max_report_age_seconds?: number) {
    if (
      typeof max_report_age_seconds !== 'number' ||
      !Number.isFinite(max_report_age_seconds)
    ) {
      return this.p2p_config.score_default_max_report_age_seconds;
    }

    return Math.max(15, Math.min(86_400, Math.trunc(max_report_age_seconds)));
  }

  private async read_stream_payload(stream: any) {
    if (!stream?.source) {
      return '';
    }

    const chunks: Uint8Array[] = [];

    for await (const chunk of stream.source as AsyncIterable<StreamChunk>) {
      chunks.push(normalize_chunk(chunk));
    }

    if (chunks.length === 0) {
      return '';
    }

    return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))).toString(
      'utf8',
    );
  }

  private parse_listen_addresses(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .filter((item): item is string => typeof item === 'string')
      .map((item) => item.trim())
      .filter(Boolean);
  }

  private is_hex(value: string) {
    return /^0x[0-9a-fA-F]+$/.test(value.trim());
  }

  private normalize_hex(value: string) {
    const trimmed_value = value.trim().toLowerCase();
    return trimmed_value.startsWith('0x')
      ? trimmed_value
      : `0x${trimmed_value}`;
  }

  private normalize_bootstrap_address(address: string) {
    let normalized_address = address.trim();

    if (!normalized_address.startsWith('/')) {
      normalized_address = `/${normalized_address}`;
    }

    normalized_address = normalized_address.replace(
      /\/p2p\/([^/]+)\/p2p\/\1$/,
      '/p2p/$1',
    );

    return normalized_address;
  }

  private extract_peer_id_from_multiaddr(address: string) {
    const match = address.match(/\/p2p\/([^/]+)$/);
    return match ? match[1] : undefined;
  }

  private trace_event(
    event_type: string,
    severity: 'error' | 'info' | 'warn',
    details?: unknown,
    peer_id?: string,
  ) {
    this.network_trace_service.record_event({
      details,
      event_type,
      peer_id,
      severity,
      source: 'p2p',
    });
  }
}
