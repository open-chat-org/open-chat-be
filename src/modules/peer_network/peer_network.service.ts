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
  PEER_DM_DELETE_PROTOCOL,
  PEER_DM_PRESENCE_PROTOCOL,
  PEER_DM_PULL_PROTOCOL,
  PEER_DM_REPLICATE_PROTOCOL,
  PEER_DB_SYNC_FETCH_BY_KEYS_PROTOCOL,
  PEER_DB_SYNC_FETCH_PROTOCOL,
  PEER_DB_SYNC_MANIFEST_PROTOCOL,
  PEER_DB_SYNC_VERIFY_PROTOCOL,
  PEER_DM_DELETE_GOSSIP_PROTOCOL,
  PEER_SERVER_SCORE_GOSSIP_PROTOCOL,
} from './constants/peer_protocol.constants';
import {
  ServerCandidate,
  ServerScoreReportPayload,
  ServerScoreReportValidationResult,
} from './types/server_score.types';
import {
  DmDeleteGossipEvent,
  SyncFetchByKeysRequest,
  SyncFetchRequest,
  SyncManifestRequest,
  SyncVerifyBatchRequest,
} from './sync/types/sync_wire.types';
import {
  InterServerDmCallbacks,
  InterServerDmDeleteAckPayload,
  InterServerDmDeleteRequestPayload,
  InterServerDmPresenceAnnouncePayload,
  InterServerDmPresenceQueryPayload,
  InterServerDmPresenceResponsePayload,
  InterServerDmPullRequestPayload,
  InterServerDmPullResponsePayload,
  InterServerDmReplicateAckPayload,
  InterServerDmReplicatePayload,
  ReplicateToPeersInput,
  ReplicateToPeersResult,
  SignedInterServerDmEvent,
} from './types/inter_server_dm.types';
import {
  create_server_score_signature,
  verify_server_score_signature,
} from './utils/server_score_signature.util';
import { DmDeleteGossipService } from './sync/services/dm_delete_gossip.service';
import { StartupSyncService } from './sync/services/startup_sync.service';
import { TableSyncRunnerService } from './sync/services/table_sync_runner.service';
import {
  create_inter_server_dm_signature,
  verify_inter_server_dm_signature,
} from './utils/inter_server_dm_signature.util';
import { get_inter_server_dm_config } from '../../config/inter_server_dm.config';
import { createHash, randomUUID } from 'node:crypto';

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

type KeepAliveRole = 'bootstrap' | 'core';

type DeleteRetryItem = {
  attempts: number;
  next_attempt_at_ms: number;
  payload: InterServerDmDeleteRequestPayload;
  peer_id: string;
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
  private static readonly KEEP_ALIVE_BOOTSTRAP_TAG =
    'keep-alive-open-chat-bootstrap';
  private static readonly KEEP_ALIVE_CORE_TAG = 'keep-alive-open-chat-core';

  private readonly logger = new Logger(PeerNetworkService.name);
  private readonly inter_server_dm_config = get_inter_server_dm_config();
  private readonly p2p_config = get_p2p_config();
  private readonly local_peer_observations = new Map<
    string,
    LocalPeerObservation
  >();
  private libp2p_node: any = null;
  private readonly keep_alive_bootstrap_peer_ids = new Set<string>();
  private readonly keep_alive_bootstrap_address_by_peer_id = new Map<
    string,
    string
  >();
  private readonly keep_alive_desired_peers = new Map<string, KeepAliveRole>();
  private readonly keep_alive_dialing_peer_ids = new Set<string>();
  private keep_alive_redial_in_flight = false;
  private keep_alive_reconcile_in_flight = false;
  private keep_alive_redial_timer: NodeJS.Timeout | null = null;
  private keep_alive_reconcile_timer: NodeJS.Timeout | null = null;
  private keep_alive_set_signature = '';
  private readonly inter_server_dm_event_dedupe = new Map<string, number>();
  private readonly inter_server_dm_delete_retry_queue = new Map<
    string,
    DeleteRetryItem
  >();
  private inter_server_dm_callbacks: InterServerDmCallbacks | null = null;
  private inter_server_dm_delete_retry_timer: NodeJS.Timeout | null = null;
  private peer_id_from_string: ((peer_id: string) => any) | null = null;
  private multiaddr_factory: ((address: string) => any) | null = null;
  private score_gossip_timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly dm_delete_gossip_service: DmDeleteGossipService,
    private readonly network_trace_service: NetworkTraceService,
    private readonly prisma_service: PrismaService,
    private readonly server_identity_service: ServerIdentityService,
    private readonly startup_sync_service: StartupSyncService,
    private readonly table_sync_runner_service: TableSyncRunnerService,
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
      { peerIdFromString },
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
      import('@libp2p/peer-id'),
    ]);

    this.multiaddr_factory = multiaddr;
    this.peer_id_from_string = peerIdFromString;

    this.libp2p_node = await createLibp2p({
      addresses: {
        listen: [this.p2p_config.listen],
      },
      transports: [tcp(), quic()],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      connectionManager: {
        dialTimeout: this.p2p_config.dial_timeout_ms,
        maxConnections: Math.max(32, this.p2p_config.target_peers * 2),
        maxParallelReconnects: this.p2p_config.max_parallel_reconnects,
        maxParallelDials: Math.max(25, this.p2p_config.target_peers),
        maxIncomingPendingConnections: 20,
        reconnectRetries: this.p2p_config.reconnect_retries,
        reconnectRetryInterval: this.p2p_config.reconnect_retry_interval_ms,
        reconnectBackoffFactor: this.p2p_config.reconnect_backoff_factor,
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
    this.inter_server_dm_delete_retry_timer = setInterval(() => {
      void this.process_inter_server_dm_delete_retry_queue();
    }, this.inter_server_dm_config.delete_retry_interval_ms);

    for (const bootstrap_address of this.p2p_config.bootstrap) {
      const normalized_address =
        this.normalize_bootstrap_address(bootstrap_address);
      const peer_id = this.extract_peer_id_from_multiaddr(normalized_address);
      if (peer_id) {
        this.keep_alive_bootstrap_peer_ids.add(peer_id);
        this.keep_alive_bootstrap_address_by_peer_id.set(
          peer_id,
          normalized_address,
        );
      }
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
    await this.reconcile_keep_alive_tags('startup');
    await this.redial_keep_alive_peers();

    if (this.p2p_config.keep_alive_enabled) {
      this.keep_alive_reconcile_timer = setInterval(() => {
        void this.reconcile_keep_alive_tags('periodic');
      }, this.p2p_config.keep_alive_reconcile_ms);
      this.keep_alive_redial_timer = setInterval(() => {
        void this.redial_keep_alive_peers();
      }, this.p2p_config.keep_alive_redial_ms);
    }

    await this.run_startup_sync_with_timeout();
  }

  async onModuleDestroy() {
    if (this.inter_server_dm_delete_retry_timer) {
      clearInterval(this.inter_server_dm_delete_retry_timer);
    }

    if (this.keep_alive_reconcile_timer) {
      clearInterval(this.keep_alive_reconcile_timer);
    }

    if (this.keep_alive_redial_timer) {
      clearInterval(this.keep_alive_redial_timer);
    }

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

  get_local_peer_id_value() {
    return this.get_local_peer_id();
  }

  register_inter_server_dm_callbacks(callbacks: InterServerDmCallbacks) {
    this.inter_server_dm_callbacks = callbacks;
  }

  async select_replica_peer_ids_for_user(
    public_key: string,
    count = this.inter_server_dm_config.replica_remote_count,
  ) {
    const normalized_count = Math.max(0, Math.trunc(count));

    if (normalized_count === 0) {
      return [] as string[];
    }

    const closest_peer_ids = await this.get_closest_peer_ids_for_key(
      public_key,
      Math.max(normalized_count * 3, normalized_count + 3),
    );
    const local_peer_id = this.get_local_peer_id();
    const selected_peer_ids: string[] = [];

    for (const peer_id of closest_peer_ids) {
      if (
        peer_id === local_peer_id ||
        peer_id === 'unknown_peer' ||
        selected_peer_ids.includes(peer_id)
      ) {
        continue;
      }

      selected_peer_ids.push(peer_id);

      if (selected_peer_ids.length >= normalized_count) {
        break;
      }
    }

    return selected_peer_ids;
  }

  async replicate_direct_message_to_peers(
    input: ReplicateToPeersInput,
  ): Promise<ReplicateToPeersResult> {
    const unique_peer_ids = Array.from(
      new Set(
        input.peer_ids.filter(
          (peer_id) =>
            peer_id &&
            peer_id !== 'unknown_peer' &&
            peer_id !== this.get_local_peer_id(),
        ),
      ),
    );
    const acknowledged_peer_ids: string[] = [];
    const failed_peer_ids: string[] = [];

    for (const peer_id of unique_peer_ids) {
      try {
        const response = await this.request_signed_inter_server_dm_event<
          InterServerDmReplicatePayload,
          InterServerDmReplicateAckPayload
        >(
          peer_id,
          PEER_DM_REPLICATE_PROTOCOL,
          'dm.replicate.request',
          {
            ...input.payload,
            replica_peer_ids: Array.from(
              new Set(input.payload.replica_peer_ids ?? []),
            ),
          },
          'dm.replicate.ack',
        );

        if (response?.stored) {
          acknowledged_peer_ids.push(peer_id);
          continue;
        }

        failed_peer_ids.push(peer_id);
      } catch (error) {
        failed_peer_ids.push(peer_id);
        this.trace_event(
          'p2p.dm_replicate_failed',
          'warn',
          {
            error: error instanceof Error ? error.message : String(error),
          },
          peer_id,
        );
      }
    }

    const quorum_target = Math.min(
      this.inter_server_dm_config.replica_remote_quorum,
      Math.max(1, unique_peer_ids.length),
    );
    const quorum_met =
      !input.require_quorum || acknowledged_peer_ids.length >= quorum_target;

    this.trace_event('p2p.dm_replicate_result', 'info', {
      acknowledged_count: acknowledged_peer_ids.length,
      failed_count: failed_peer_ids.length,
      quorum_met,
      quorum_target,
      require_quorum: input.require_quorum,
    });

    return {
      acknowledged_peer_ids,
      failed_peer_ids,
      quorum_met,
    };
  }

  async announce_user_presence(public_key: string, replica_peer_ids?: string[]) {
    await this.cleanup_expired_presence_records();

    const local_peer_id = this.get_local_peer_id();
    const expires_at = new Date(
      Date.now() + this.inter_server_dm_config.presence_ttl_ms,
    );

    await this.prisma_service.userPresence.upsert({
      where: {
        public_key_server_peer_id: {
          public_key,
          server_peer_id: local_peer_id,
        },
      },
      update: {
        expires_at,
        observed_at: new Date(),
      },
      create: {
        expires_at,
        public_key,
        server_peer_id: local_peer_id,
      },
    });

    const targets = Array.from(
      new Set(
        (replica_peer_ids ?? []).filter(
          (peer_id) => peer_id && peer_id !== local_peer_id,
        ),
      ),
    );
    const payload: InterServerDmPresenceAnnouncePayload = {
      expires_at: expires_at.toISOString(),
      public_key,
      server_peer_id: local_peer_id,
    };

    await Promise.all(
      targets.map(async (peer_id) => {
        try {
          await this.request_signed_inter_server_dm_event<
            InterServerDmPresenceAnnouncePayload,
            { accepted: boolean }
          >(
            peer_id,
            PEER_DM_PRESENCE_PROTOCOL,
            'dm.presence.announce',
            payload,
            'dm.presence.ack',
          );
        } catch (error) {
          this.trace_event(
            'p2p.dm_presence_announce_failed',
            'warn',
            {
              error: error instanceof Error ? error.message : String(error),
            },
            peer_id,
          );
        }
      }),
    );
  }

  async query_user_presence(public_key: string, replica_peer_ids?: string[]) {
    await this.cleanup_expired_presence_records();

    const local_presence = await this.prisma_service.userPresence.findMany({
      where: {
        expires_at: {
          gt: new Date(),
        },
        public_key,
      },
      select: {
        server_peer_id: true,
      },
    });
    const online_server_peer_ids = new Set(
      local_presence.map((presence) => presence.server_peer_id),
    );
    const targets = Array.from(
      new Set((replica_peer_ids ?? []).filter((peer_id) => Boolean(peer_id))),
    );

    await Promise.all(
      targets.map(async (peer_id) => {
        try {
          const response = await this.request_signed_inter_server_dm_event<
            InterServerDmPresenceQueryPayload,
            InterServerDmPresenceResponsePayload
          >(
            peer_id,
            PEER_DM_PRESENCE_PROTOCOL,
            'dm.presence.query',
            {
              public_key,
            },
            'dm.presence.response',
          );

          for (const server_peer_id of response.online_server_peer_ids) {
            if (server_peer_id) {
              online_server_peer_ids.add(server_peer_id);
            }
          }
        } catch (error) {
          this.trace_event(
            'p2p.dm_presence_query_failed',
            'warn',
            {
              error: error instanceof Error ? error.message : String(error),
              public_key,
            },
            peer_id,
          );
        }
      }),
    );

    return {
      online_server_peer_ids: Array.from(online_server_peer_ids),
      public_key,
    };
  }

  async pull_pending_messages_from_replicas(
    public_key: string,
    replica_peer_ids: string[],
  ) {
    const items_by_id = new Map<string, InterServerDmReplicatePayload>();
    const targets = Array.from(
      new Set(
        replica_peer_ids.filter(
          (peer_id) => peer_id && peer_id !== this.get_local_peer_id(),
        ),
      ),
    );

    for (const peer_id of targets) {
      let cursor: string | null = null;

      while (true) {
        try {
          const response = await this.request_signed_inter_server_dm_event<
            InterServerDmPullRequestPayload,
            InterServerDmPullResponsePayload
          >(
            peer_id,
            PEER_DM_PULL_PROTOCOL,
            'dm.pull.request',
            {
              cursor,
              limit: this.inter_server_dm_config.pull_batch_size,
              public_key,
            },
            'dm.pull.response',
          );

          for (const item of response.items) {
            if (item?.id && !items_by_id.has(item.id)) {
              items_by_id.set(item.id, item);
            }
          }

          if (!response.next_cursor) {
            break;
          }

          cursor = response.next_cursor;
        } catch (error) {
          this.trace_event(
            'p2p.dm_pull_failed',
            'warn',
            {
              error: error instanceof Error ? error.message : String(error),
              public_key,
            },
            peer_id,
          );
          break;
        }
      }
    }

    return Array.from(items_by_id.values());
  }

  async send_targeted_dm_delete(input: InterServerDmDeleteRequestPayload) {
    const target_peer_ids = Array.from(
      new Set([
        input.origin_server_peer_id,
        ...(input.replica_peer_ids ?? []),
      ]).values(),
    ).filter((peer_id) => Boolean(peer_id) && peer_id !== 'unknown_peer');

    await Promise.all(
      target_peer_ids.map(async (peer_id) => {
        if (!peer_id) {
          return;
        }

        const success = await this.send_dm_delete_request_to_peer(peer_id, input);

        if (!success) {
          this.enqueue_dm_delete_retry(peer_id, input, 0);
        }
      }),
    );
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

  async publish_dm_delete_gossip(
    message_id: string,
    recipient_public_key: string,
  ) {
    if (this.libp2p_node == null || !this.p2p_config.sync_enabled) {
      return;
    }

    const local_peer_id = this.get_local_peer_id();
    const server_identity = await this.server_identity_service.get_public_key();
    const event = await this.dm_delete_gossip_service.create_event({
      local_peer_id,
      max_hops: Math.max(1, this.p2p_config.sync_validator_target),
      message_id,
      recipient_public_key,
      reporter_server_public_key: server_identity.public_key,
      sign_message: async (message) =>
        this.server_identity_service.sign_message(message),
    });

    await this.broadcast_dm_delete_gossip_event(event);
  }

  private register_protocol_handlers() {
    if (this.libp2p_node == null) {
      return;
    }

    this.libp2p_node.handle(
      PEER_HELLO_PROTOCOL,
      async (stream: any, connection: any) => {
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

        if (stream != null) {
          await this.write_stream_payload(stream, 'hello-ack');
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
      async (stream: any, connection: any) => {
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

    this.libp2p_node.handle(
      PEER_DB_SYNC_MANIFEST_PROTOCOL,
      async (stream: any, connection: any) => {
        const remote_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const payload = this.parse_json(payload_text);

        if (!this.is_sync_manifest_request(payload)) {
          this.trace_event(
            'p2p.sync_manifest_rejected',
            'warn',
            {
              reason: 'Invalid sync manifest request payload.',
            },
            remote_peer_id,
          );
          return;
        }

        const response =
          await this.table_sync_runner_service.create_manifest_response(
            payload.run_id,
            this.get_local_peer_id(),
          );

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DB_SYNC_FETCH_PROTOCOL,
      async (stream: any, connection: any) => {
        const remote_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const payload = this.parse_json(payload_text);

        if (!this.is_sync_fetch_request(payload)) {
          this.trace_event(
            'p2p.sync_fetch_rejected',
            'warn',
            {
              reason: 'Invalid sync fetch request payload.',
            },
            remote_peer_id,
          );
          return;
        }

        const response =
          await this.table_sync_runner_service.handle_fetch_request(payload);

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DB_SYNC_VERIFY_PROTOCOL,
      async (stream: any, connection: any) => {
        const remote_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const payload = this.parse_json(payload_text);

        if (!this.is_sync_verify_request(payload)) {
          this.trace_event(
            'p2p.sync_verify_rejected',
            'warn',
            {
              reason: 'Invalid sync verify request payload.',
            },
            remote_peer_id,
          );
          return;
        }

        const response =
          await this.table_sync_runner_service.handle_verify_request(payload);

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DB_SYNC_FETCH_BY_KEYS_PROTOCOL,
      async (stream: any, connection: any) => {
        const remote_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const payload = this.parse_json(payload_text);

        if (!this.is_sync_fetch_by_keys_request(payload)) {
          this.trace_event(
            'p2p.sync_fetch_by_keys_rejected',
            'warn',
            {
              reason: 'Invalid sync fetch-by-keys request payload.',
            },
            remote_peer_id,
          );
          return;
        }

        const response =
          await this.table_sync_runner_service.handle_fetch_by_keys_request(
            payload,
          );

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DM_DELETE_GOSSIP_PROTOCOL,
      async (stream: any, connection: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const payload = this.parse_json(payload_text);
        const event = this.dm_delete_gossip_service.parse_event(payload);

        if (!event) {
          this.trace_event(
            'p2p.dm_delete_gossip_rejected',
            'warn',
            {
              reason: 'Invalid DM delete gossip payload.',
            },
            sender_peer_id,
          );
          return;
        }

        if (
          !this.dm_delete_gossip_service.should_process_event(event.event_id)
        ) {
          return;
        }

        const is_valid_signature =
          await this.dm_delete_gossip_service.verify_event_signature(event);

        if (!is_valid_signature) {
          this.trace_event(
            'p2p.dm_delete_gossip_rejected',
            'warn',
            {
              event_id: event.event_id,
              reason: 'DM delete gossip signature verification failed.',
            },
            sender_peer_id,
          );
          return;
        }

        await this.prisma_service.directMessage.deleteMany({
          where: {
            id: event.message_id,
          },
        });
        this.trace_event(
          'p2p.dm_delete_gossip_applied',
          'info',
          {
            event_id: event.event_id,
            hop_count: event.hop_count,
            message_id: event.message_id,
            recipient_public_key: event.recipient_public_key,
          },
          sender_peer_id,
        );

        if (!this.dm_delete_gossip_service.can_forward(event)) {
          await this.write_stream_payload(
            stream,
            JSON.stringify({
              event_id: event.event_id,
              status: 'ok',
              type: 'dm.delete.gossip.ack',
            }),
          );
          return;
        }

        await this.broadcast_dm_delete_gossip_event(
          this.dm_delete_gossip_service.create_forwarded_event(event),
          sender_peer_id,
        );
        await this.write_stream_payload(
          stream,
          JSON.stringify({
            event_id: event.event_id,
            status: 'ok',
            type: 'dm.delete.gossip.ack',
          }),
        );
      },
    );

    this.libp2p_node.handle(
      PEER_DM_REPLICATE_PROTOCOL,
      async (stream: any, connection: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const signed_event = this.parse_json(payload_text) as
          | SignedInterServerDmEvent<unknown>
          | null;

        const parsed_event = await this.verify_and_extract_signed_inter_server_dm_event(
          signed_event,
          sender_peer_id,
          'dm.replicate.request',
        );

        if (!parsed_event) {
          return;
        }

        if (
          !this.should_process_inter_server_dm_event(
            parsed_event.event.event_id,
            'dm.replicate.request',
          )
        ) {
          const duplicate_response = await this.create_signed_inter_server_dm_event(
            'dm.replicate.ack',
            {
              id: (parsed_event.payload as any).id ?? '',
              stored: true,
            } satisfies InterServerDmReplicateAckPayload,
          );
          await this.write_stream_payload(
            stream,
            JSON.stringify(duplicate_response),
          );
          return;
        }

        const replicate_payload = this.parse_inter_server_dm_replicate_payload(
          parsed_event.payload,
        );
        const replicate_response =
          await this.inter_server_dm_callbacks?.on_replicate_request(
            replicate_payload,
            sender_peer_id,
          );
        const response = await this.create_signed_inter_server_dm_event(
          'dm.replicate.ack',
          replicate_response ?? {
            id: replicate_payload.id,
            stored: false,
          },
        );

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DM_PRESENCE_PROTOCOL,
      async (stream: any, connection: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const signed_event = this.parse_json(payload_text) as
          | SignedInterServerDmEvent<unknown>
          | null;

        const parsed_event = await this.verify_and_extract_signed_inter_server_dm_event(
          signed_event,
          sender_peer_id,
        );

        if (!parsed_event) {
          return;
        }

        if (parsed_event.event.type === 'dm.presence.announce') {
          const presence_payload = this.parse_inter_server_dm_presence_announce_payload(
            parsed_event.payload,
          );

          if (
            this.should_process_inter_server_dm_event(
              parsed_event.event.event_id,
              'dm.presence.announce',
            )
          ) {
            await this.prisma_service.userPresence.upsert({
              where: {
                public_key_server_peer_id: {
                  public_key: presence_payload.public_key,
                  server_peer_id: presence_payload.server_peer_id,
                },
              },
              update: {
                expires_at: new Date(presence_payload.expires_at),
                observed_at: new Date(),
              },
              create: {
                expires_at: new Date(presence_payload.expires_at),
                public_key: presence_payload.public_key,
                server_peer_id: presence_payload.server_peer_id,
              },
            });
          }

          const response = await this.create_signed_inter_server_dm_event(
            'dm.presence.ack',
            {
              accepted: true,
            },
          );
          await this.write_stream_payload(stream, JSON.stringify(response));
          return;
        }

        if (parsed_event.event.type === 'dm.presence.query') {
          const presence_query_payload = this.parse_inter_server_dm_presence_query_payload(
            parsed_event.payload,
          );

          await this.cleanup_expired_presence_records();

          const presence_rows = await this.prisma_service.userPresence.findMany({
            where: {
              expires_at: {
                gt: new Date(),
              },
              public_key: presence_query_payload.public_key,
            },
            select: {
              server_peer_id: true,
            },
          });
          const response_payload: InterServerDmPresenceResponsePayload = {
            online_server_peer_ids: presence_rows.map(
              (presence_row) => presence_row.server_peer_id,
            ),
            public_key: presence_query_payload.public_key,
          };
          const response = await this.create_signed_inter_server_dm_event(
            'dm.presence.response',
            response_payload,
          );

          await this.write_stream_payload(stream, JSON.stringify(response));
          return;
        }

        this.trace_event(
          'p2p.dm_presence_rejected',
          'warn',
          {
            reason: `Unsupported presence event type: ${parsed_event.event.type}`,
          },
          sender_peer_id,
        );
      },
    );

    this.libp2p_node.handle(
      PEER_DM_PULL_PROTOCOL,
      async (stream: any, connection: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const signed_event = this.parse_json(payload_text) as
          | SignedInterServerDmEvent<unknown>
          | null;

        const parsed_event = await this.verify_and_extract_signed_inter_server_dm_event(
          signed_event,
          sender_peer_id,
          'dm.pull.request',
        );

        if (!parsed_event) {
          return;
        }

        const pull_payload = this.parse_inter_server_dm_pull_request_payload(
          parsed_event.payload,
        );
        const pull_response =
          await this.inter_server_dm_callbacks?.on_pull_request(
            pull_payload,
            sender_peer_id,
          );
        const response = await this.create_signed_inter_server_dm_event(
          'dm.pull.response',
          pull_response ?? {
            items: [],
            next_cursor: null,
            public_key: pull_payload.public_key,
          },
        );

        await this.write_stream_payload(stream, JSON.stringify(response));
      },
    );

    this.libp2p_node.handle(
      PEER_DM_DELETE_PROTOCOL,
      async (stream: any, connection: any) => {
        const sender_peer_id =
          connection?.remotePeer?.toString?.() ?? 'unknown_peer';
        const payload_text = await this.read_stream_payload(stream);
        const signed_event = this.parse_json(payload_text) as
          | SignedInterServerDmEvent<unknown>
          | null;

        const parsed_event = await this.verify_and_extract_signed_inter_server_dm_event(
          signed_event,
          sender_peer_id,
          'dm.delete.request',
        );

        if (!parsed_event) {
          return;
        }

        const delete_payload = this.parse_inter_server_dm_delete_request_payload(
          parsed_event.payload,
        );

        if (
          !this.should_process_inter_server_dm_event(
            parsed_event.event.event_id,
            'dm.delete.request',
          )
        ) {
          const duplicate_response = await this.create_signed_inter_server_dm_event(
            'dm.delete.ack',
            {
              deleted: true,
              message_id: delete_payload.message_id,
            } satisfies InterServerDmDeleteAckPayload,
          );
          await this.write_stream_payload(
            stream,
            JSON.stringify(duplicate_response),
          );
          return;
        }

        const delete_response =
          await this.inter_server_dm_callbacks?.on_delete_request(
            delete_payload,
            sender_peer_id,
          );
        const response = await this.create_signed_inter_server_dm_event(
          'dm.delete.ack',
          delete_response ?? {
            deleted: false,
            message_id: delete_payload.message_id,
          },
        );

        await this.write_stream_payload(stream, JSON.stringify(response));
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
      void this.reconcile_keep_alive_tags('peer_event');
      void this.startup_sync_service.run_sync(
        'peer_reconnect',
        this.build_sync_runtime(),
        false,
      );
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
      void this.reconcile_keep_alive_tags('peer_event');
      void this.redial_keep_alive_peers();
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

  private async reconcile_keep_alive_tags(
    trigger: 'peer_event' | 'periodic' | 'startup',
  ) {
    if (!this.p2p_config.keep_alive_enabled || this.libp2p_node == null) {
      return;
    }

    if (this.keep_alive_reconcile_in_flight) {
      return;
    }

    this.keep_alive_reconcile_in_flight = true;

    try {
      const desired_keep_alive_peers =
        await this.build_desired_keep_alive_peers();
      const peer_store = this.libp2p_node?.peerStore;

      if (!peer_store) {
        return;
      }

      const peer_store_peers = await peer_store.all();
      const existing_keep_alive_role_by_peer_id = new Map<
        string,
        KeepAliveRole
      >();

      for (const peer of peer_store_peers) {
        const peer_id = peer?.id?.toString?.();
        if (!peer_id) {
          continue;
        }

        if (
          peer?.tags?.has?.(PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG) ===
          true
        ) {
          existing_keep_alive_role_by_peer_id.set(peer_id, 'bootstrap');
          continue;
        }

        if (peer?.tags?.has?.(PeerNetworkService.KEEP_ALIVE_CORE_TAG) === true) {
          existing_keep_alive_role_by_peer_id.set(peer_id, 'core');
        }
      }

      let applied_tag_count = 0;
      let removed_tag_count = 0;

      for (const [peer_id, role] of desired_keep_alive_peers.entries()) {
        if (existing_keep_alive_role_by_peer_id.get(peer_id) === role) {
          continue;
        }

        const peer_id_object = this.parse_peer_id(peer_id);

        if (!peer_id_object) {
          continue;
        }

        const tag_to_apply = this.get_keep_alive_tag(role);
        const tag_to_remove =
          role === 'bootstrap'
            ? PeerNetworkService.KEEP_ALIVE_CORE_TAG
            : PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG;

        await peer_store.merge(peer_id_object, {
          tags: {
            [tag_to_apply]: {
              value: role === 'bootstrap' ? 100 : 80,
            },
            [tag_to_remove]: undefined,
          },
        });
        applied_tag_count += 1;
        this.trace_event(
          'p2p.keep_alive_tag_applied',
          'info',
          {
            role,
            tag: tag_to_apply,
          },
          peer_id,
        );
      }

      for (const peer of peer_store_peers) {
        const peer_id = peer?.id?.toString?.();

        if (!peer_id || desired_keep_alive_peers.has(peer_id)) {
          continue;
        }

        const has_keep_alive_bootstrap_tag =
          peer?.tags?.has?.(PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG) ===
          true;
        const has_keep_alive_core_tag =
          peer?.tags?.has?.(PeerNetworkService.KEEP_ALIVE_CORE_TAG) === true;

        if (!has_keep_alive_bootstrap_tag && !has_keep_alive_core_tag) {
          continue;
        }

        await peer_store.merge(peer.id, {
          tags: {
            [PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG]: undefined,
            [PeerNetworkService.KEEP_ALIVE_CORE_TAG]: undefined,
          },
        });
        removed_tag_count += 1;
        this.trace_event(
          'p2p.keep_alive_tag_removed',
          'info',
          {
            removed_tags: [
              PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG,
              PeerNetworkService.KEEP_ALIVE_CORE_TAG,
            ],
          },
          peer_id,
        );
      }

      this.keep_alive_desired_peers.clear();

      for (const [peer_id, role] of desired_keep_alive_peers.entries()) {
        this.keep_alive_desired_peers.set(peer_id, role);
      }

      const keep_alive_set_signature = this.create_keep_alive_set_signature(
        this.keep_alive_desired_peers,
      );
      const set_changed = keep_alive_set_signature !== this.keep_alive_set_signature;
      this.keep_alive_set_signature = keep_alive_set_signature;

      if (set_changed || trigger !== 'periodic') {
        const bootstrap_count = Array.from(
          this.keep_alive_desired_peers.values(),
        ).filter((role) => role === 'bootstrap').length;
        const core_count = Array.from(this.keep_alive_desired_peers.values()).filter(
          (role) => role === 'core',
        ).length;

        this.trace_event('p2p.keep_alive_set_updated', 'info', {
          applied_tag_count,
          bootstrap_count,
          changed: set_changed,
          core_count,
          desired_peer_count: this.keep_alive_desired_peers.size,
          removed_tag_count,
          trigger,
        });
      }
    } catch (error) {
      this.trace_event('p2p.keep_alive_set_updated', 'warn', {
        error: error instanceof Error ? error.message : String(error),
        trigger,
      });
    } finally {
      this.keep_alive_reconcile_in_flight = false;
    }
  }

  private async redial_keep_alive_peers() {
    if (!this.p2p_config.keep_alive_enabled || this.libp2p_node == null) {
      return;
    }

    if (this.keep_alive_redial_in_flight) {
      return;
    }

    this.keep_alive_redial_in_flight = true;

    try {
      if (this.keep_alive_desired_peers.size === 0) {
        await this.reconcile_keep_alive_tags('periodic');
      }

      for (const [peer_id, role] of this.keep_alive_desired_peers.entries()) {
        if (
          this.is_peer_directly_connected(peer_id) ||
          this.keep_alive_dialing_peer_ids.has(peer_id) ||
          this.is_peer_in_dial_queue(peer_id)
        ) {
          continue;
        }

        const bootstrap_address =
          this.keep_alive_bootstrap_address_by_peer_id.get(peer_id);
        const peer_id_object = this.parse_peer_id(peer_id);

        if (!peer_id_object && !bootstrap_address) {
          this.trace_event(
            'p2p.keep_alive_redial_failed',
            'warn',
            {
              reason: 'Unable to parse peer id for keep-alive dial target.',
              role,
            },
            peer_id,
          );
          continue;
        }

        const peer_store_entry = await this.get_peer_store_peer(peer_id_object);
        const has_known_addresses =
          (peer_store_entry?.addresses?.length ?? 0) > 0 ||
          Boolean(bootstrap_address);

        if (!has_known_addresses) {
          this.trace_event(
            'p2p.keep_alive_redial_failed',
            'warn',
            {
              reason: 'Peer has no known addresses to dial.',
              role,
            },
            peer_id,
          );
          continue;
        }

        const dial_target =
          role === 'bootstrap' && bootstrap_address && this.multiaddr_factory
            ? this.multiaddr_factory(bootstrap_address)
            : peer_id_object;

        if (!dial_target) {
          this.trace_event(
            'p2p.keep_alive_redial_failed',
            'warn',
            {
              reason: 'Failed to build dial target.',
              role,
            },
            peer_id,
          );
          continue;
        }

        this.keep_alive_dialing_peer_ids.add(peer_id);
        this.note_dial_attempt(peer_id);
        this.trace_event(
          'p2p.keep_alive_redial_attempt',
          'info',
          {
            role,
          },
          peer_id,
        );

        try {
          await this.libp2p_node.dial(dial_target);
          this.note_dial_success(peer_id);
          this.trace_event(
            'p2p.keep_alive_redial_succeeded',
            'info',
            {
              role,
            },
            peer_id,
          );
        } catch (error) {
          this.note_dial_failure(peer_id);
          this.trace_event(
            'p2p.keep_alive_redial_failed',
            'warn',
            {
              error: error instanceof Error ? error.message : String(error),
              role,
            },
            peer_id,
          );
        } finally {
          this.keep_alive_dialing_peer_ids.delete(peer_id);
        }
      }
    } finally {
      this.keep_alive_redial_in_flight = false;
    }
  }

  private async build_desired_keep_alive_peers() {
    const desired_keep_alive_peers = new Map<string, KeepAliveRole>();
    const local_peer_id = this.get_local_peer_id();

    for (const bootstrap_peer_id of this.keep_alive_bootstrap_peer_ids.values()) {
      if (
        bootstrap_peer_id &&
        bootstrap_peer_id !== local_peer_id &&
        bootstrap_peer_id !== 'unknown_peer'
      ) {
        desired_keep_alive_peers.set(bootstrap_peer_id, 'bootstrap');
      }
    }

    const core_peer_ids = await this.select_core_keep_alive_peer_ids(
      local_peer_id,
      desired_keep_alive_peers,
    );

    for (const core_peer_id of core_peer_ids) {
      if (!desired_keep_alive_peers.has(core_peer_id)) {
        desired_keep_alive_peers.set(core_peer_id, 'core');
      }
    }

    return desired_keep_alive_peers;
  }

  private async select_core_keep_alive_peer_ids(
    local_peer_id: string,
    existing_peer_ids: Map<string, KeepAliveRole>,
  ) {
    const normalized_core_count = Math.max(
      0,
      Math.trunc(this.p2p_config.keep_alive_core_count),
    );

    if (normalized_core_count === 0) {
      return [] as string[];
    }

    const now = new Date();
    const max_records = Math.max(normalized_core_count * 8, 64);
    const [aggregates, server_nodes] = await Promise.all([
      this.compute_candidate_aggregates(
        this.p2p_config.score_default_max_report_age_seconds,
        now,
      ),
      this.prisma_service.serverNode.findMany({
        where: {
          peer_id: {
            not: local_peer_id,
          },
        },
        orderBy: [
          {
            is_active: 'desc',
          },
          {
            last_seen_at: 'desc',
          },
        ],
        take: max_records,
      }),
    ]);
    const connected_peer_ids = new Set(
      this.get_connected_peers().map((peer) => peer.peer_id),
    );
    const aggregate_by_peer_id = new Map(
      aggregates.map((aggregate) => [aggregate.target_peer_id, aggregate]),
    );
    const server_node_by_peer_id = new Map(
      server_nodes.map((server_node) => [server_node.peer_id, server_node]),
    );
    const candidate_peer_ids = new Set<string>();

    for (const connected_peer_id of connected_peer_ids.values()) {
      candidate_peer_ids.add(connected_peer_id);
    }

    for (const observed_peer_id of this.local_peer_observations.keys()) {
      candidate_peer_ids.add(observed_peer_id);
    }

    for (const aggregate of aggregates) {
      candidate_peer_ids.add(aggregate.target_peer_id);
    }

    for (const server_node of server_nodes) {
      candidate_peer_ids.add(server_node.peer_id);
    }

    const ranked_candidates = Array.from(candidate_peer_ids.values())
      .filter(
        (peer_id) =>
          peer_id &&
          peer_id !== local_peer_id &&
          peer_id !== 'unknown_peer' &&
          !existing_peer_ids.has(peer_id),
      )
      .map((peer_id) => {
        const aggregate = aggregate_by_peer_id.get(peer_id);
        const server_node = server_node_by_peer_id.get(peer_id);
        const observation = this.local_peer_observations.get(peer_id);
        const freshness_ms = Math.max(
          aggregate?.last_observed_at_ms ?? 0,
          server_node?.last_seen_at?.getTime?.() ?? 0,
          observation?.last_event_at_ms ?? 0,
        );

        return {
          freshness_ms,
          is_connected: connected_peer_ids.has(peer_id) ? 1 : 0,
          mean_score: aggregate?.mean_score ?? -1,
          peer_id,
          report_count: aggregate?.report_count ?? 0,
        };
      })
      .sort((left, right) => {
        if (right.mean_score !== left.mean_score) {
          return right.mean_score - left.mean_score;
        }

        if (right.report_count !== left.report_count) {
          return right.report_count - left.report_count;
        }

        if (right.is_connected !== left.is_connected) {
          return right.is_connected - left.is_connected;
        }

        if (right.freshness_ms !== left.freshness_ms) {
          return right.freshness_ms - left.freshness_ms;
        }

        return left.peer_id.localeCompare(right.peer_id);
      });

    return ranked_candidates
      .slice(0, normalized_core_count)
      .map((candidate) => candidate.peer_id);
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

    const report_batches = this.create_report_batches(reports, 25);

    await Promise.all(
      connected_peers.map(async (peer) =>
        this.send_server_score_reports_to_peer(peer.peer_id, report_batches),
      ),
    );
  }

  private async send_server_score_reports_to_peer(
    peer_id: string,
    report_batches: ServerScoreReportPayload[][],
  ) {
    if (this.libp2p_node == null) {
      return;
    }

    const peer_connection = this.get_connected_peer_connection(peer_id);

    if (!peer_connection?.remotePeer) {
      return;
    }

    let sent_report_count = 0;

    for (const batch of report_batches) {
      try {
        const stream = await this.open_peer_protocol_stream(
          peer_connection,
          PEER_SERVER_SCORE_GOSSIP_PROTOCOL,
        );

        await this.write_stream_payload(stream, JSON.stringify(batch));
        sent_report_count += batch.length;
      } catch (error) {
        const error_message =
          error instanceof Error ? error.message : String(error);
        this.trace_event(
          'p2p.score_gossip_send_failed',
          'warn',
          {
            batch_size: batch.length,
            error: error_message,
            sent_report_count,
            total_report_count: report_batches.reduce(
              (count, current_batch) => count + current_batch.length,
              0,
            ),
          },
          peer_id,
        );
        return;
      }
    }

    this.trace_event(
      'p2p.score_gossip_sent',
      'info',
      {
        report_count: sent_report_count,
      },
      peer_id,
    );
  }

  private async open_peer_protocol_stream(
    peer_connection: any,
    protocol: string,
  ) {
    const errors: string[] = [];
    const new_stream = peer_connection?.newStream;
    const remote_peer = peer_connection?.remotePeer;

    if (typeof new_stream === 'function') {
      try {
        const result = await new_stream.call(peer_connection, [protocol]);
        const stream = this.extract_protocol_stream(result);

        if (stream) {
          return stream;
        }

        errors.push('connection.newStream returned unsupported stream shape');
      } catch (error) {
        errors.push(
          `connection.newStream failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    if (remote_peer) {
      try {
        const result = await this.libp2p_node.dialProtocol(remote_peer, [
          protocol,
        ]);
        const stream = this.extract_protocol_stream(result);

        if (stream) {
          return stream;
        }

        errors.push('libp2p.dialProtocol returned unsupported stream shape');
      } catch (error) {
        errors.push(
          `libp2p.dialProtocol failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }

      try {
        const refreshed_connection = await this.libp2p_node.dial(remote_peer);
        const result = await refreshed_connection.newStream([protocol]);
        const stream = this.extract_protocol_stream(result);

        if (stream) {
          return stream;
        }

        errors.push(
          'libp2p.dial + connection.newStream returned unsupported stream shape',
        );
      } catch (error) {
        errors.push(
          `libp2p.dial + connection.newStream failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    throw new Error(
      `Failed to open protocol stream for connected peer. Attempts: ${errors.join(' | ')}`,
    );
  }

  private async open_peer_protocol_stream_for_peer_id(
    peer_id: string,
    protocol: string,
  ) {
    const existing_connection = this.get_connected_peer_connection(peer_id);

    if (existing_connection) {
      return this.open_peer_protocol_stream(existing_connection, protocol);
    }

    const peer_id_object = this.parse_peer_id(peer_id);

    if (!peer_id_object) {
      throw new Error(`Unable to parse peer id: ${peer_id}`);
    }

    const connection = await this.libp2p_node.dial(peer_id_object);

    return this.open_peer_protocol_stream(connection, protocol);
  }

  private extract_protocol_stream(result: any) {
    if (result?.sink && result?.source) {
      return result;
    }

    if (result?.stream?.sink && result?.stream?.source) {
      return result.stream;
    }

    if (
      result?.send &&
      typeof result.send === 'function' &&
      typeof result[Symbol.asyncIterator] === 'function'
    ) {
      return result;
    }

    if (
      result?.stream?.send &&
      typeof result.stream.send === 'function' &&
      typeof result.stream[Symbol.asyncIterator] === 'function'
    ) {
      return result.stream;
    }

    return null;
  }

  private build_sync_runtime() {
    return {
      connected_peer_ids: this.get_connected_peers().map(
        (peer) => peer.peer_id,
      ),
      local_peer_id: this.get_local_peer_id(),
      request_peer: async (
        peer_id: string,
        protocol: string,
        payload: unknown,
      ) => this.request_peer_protocol_json(peer_id, protocol, payload),
    };
  }

  private async run_startup_sync_with_timeout() {
    if (!this.p2p_config.sync_enabled) {
      return;
    }

    const sync_promise = this.startup_sync_service.run_sync(
      'startup',
      this.build_sync_runtime(),
      true,
    );
    const timeout_promise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(
          new Error(
            `Startup sync timed out after ${this.p2p_config.sync_startup_timeout_ms}ms.`,
          ),
        );
      }, this.p2p_config.sync_startup_timeout_ms);
    });

    try {
      await Promise.race([sync_promise, timeout_promise]);
    } catch (error) {
      this.logger.warn(
        error instanceof Error ? error.message : 'Startup sync timed out.',
      );
      this.trace_event('p2p.sync_startup_timeout', 'warn', {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private async request_peer_protocol_json(
    peer_id: string,
    protocol: string,
    payload: unknown,
  ) {
    const peer_connection = this.get_connected_peer_connection(peer_id);

    if (!peer_connection) {
      throw new Error(`Peer ${peer_id} is not connected.`);
    }

    const stream = await this.open_peer_protocol_stream(
      peer_connection,
      protocol,
    );

    await this.write_stream_payload(stream, JSON.stringify(payload));

    const response_text = await this.read_stream_payload(stream);
    const parsed_payload = this.parse_json(response_text);

    if (parsed_payload == null) {
      throw new Error(
        `Failed to parse protocol response from peer ${peer_id} on ${protocol}.`,
      );
    }

    return parsed_payload;
  }

  private async request_peer_protocol_json_for_peer_id(
    peer_id: string,
    protocol: string,
    payload: unknown,
    timeout_ms = this.inter_server_dm_config.p2p_request_timeout_ms,
  ) {
    const request_promise = (async () => {
      const stream = await this.open_peer_protocol_stream_for_peer_id(
        peer_id,
        protocol,
      );

      await this.write_stream_payload(stream, JSON.stringify(payload));

      const response_text = await this.read_stream_payload(stream);
      const parsed_payload = this.parse_json(response_text);

      if (parsed_payload == null) {
        throw new Error(
          `Failed to parse protocol response from peer ${peer_id} on ${protocol}.`,
        );
      }

      return parsed_payload;
    })();
    const timeout_promise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(
          new Error(
            `Peer protocol request timed out after ${timeout_ms}ms for ${peer_id} on ${protocol}.`,
          ),
        );
      }, timeout_ms);
    });

    return Promise.race([request_promise, timeout_promise]);
  }

  private async broadcast_dm_delete_gossip_event(
    event: DmDeleteGossipEvent,
    exclude_peer_id?: string,
  ) {
    const connected_peers = this.get_connected_peers().filter(
      (peer) => peer.peer_id !== exclude_peer_id,
    );

    await Promise.all(
      connected_peers.map(async (peer) => {
        try {
          const peer_connection = this.get_connected_peer_connection(
            peer.peer_id,
          );

          if (!peer_connection) {
            return;
          }

          const stream = await this.open_peer_protocol_stream(
            peer_connection,
            PEER_DM_DELETE_GOSSIP_PROTOCOL,
          );
          await this.write_stream_payload(stream, JSON.stringify(event));
          this.trace_event(
            'p2p.dm_delete_gossip_sent',
            'info',
            {
              event_id: event.event_id,
              hop_count: event.hop_count,
              message_id: event.message_id,
            },
            peer.peer_id,
          );
        } catch (error) {
          this.trace_event(
            'p2p.dm_delete_gossip_send_failed',
            'warn',
            {
              error: error instanceof Error ? error.message : String(error),
              event_id: event.event_id,
              message_id: event.message_id,
            },
            peer.peer_id,
          );
        }
      }),
    );
  }

  private parse_json(value: string) {
    if (!value.trim()) {
      return null;
    }

    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  private is_sync_manifest_request(
    value: unknown,
  ): value is SyncManifestRequest {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.manifest.request' &&
      typeof candidate.run_id === 'string'
    );
  }

  private is_sync_fetch_request(value: unknown): value is SyncFetchRequest {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.fetch.request' &&
      typeof candidate.run_id === 'string' &&
      typeof candidate.table === 'string' &&
      typeof candidate.limit === 'number'
    );
  }

  private is_sync_verify_request(
    value: unknown,
  ): value is SyncVerifyBatchRequest {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.verify.request' &&
      typeof candidate.run_id === 'string' &&
      typeof candidate.table === 'string' &&
      Array.isArray(candidate.rows)
    );
  }

  private is_sync_fetch_by_keys_request(
    value: unknown,
  ): value is SyncFetchByKeysRequest {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.fetch_by_keys.request' &&
      typeof candidate.run_id === 'string' &&
      typeof candidate.table === 'string' &&
      Array.isArray(candidate.keys)
    );
  }

  private create_report_batches(
    reports: ServerScoreReportPayload[],
    batch_size: number,
  ) {
    if (batch_size <= 0) {
      return [reports];
    }

    const batches: ServerScoreReportPayload[][] = [];

    for (let index = 0; index < reports.length; index += batch_size) {
      batches.push(reports.slice(index, index + batch_size));
    }

    return batches;
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

    const existing_reporter_node =
      await this.prisma_service.serverNode.findUnique({
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

  private create_keep_alive_set_signature(
    keep_alive_peers: Map<string, KeepAliveRole>,
  ) {
    return Array.from(keep_alive_peers.entries())
      .sort(([left_peer_id], [right_peer_id]) =>
        left_peer_id.localeCompare(right_peer_id),
      )
      .map(([peer_id, role]) => `${peer_id}:${role}`)
      .join('|');
  }

  private get_keep_alive_tag(role: KeepAliveRole) {
    return role === 'bootstrap'
      ? PeerNetworkService.KEEP_ALIVE_BOOTSTRAP_TAG
      : PeerNetworkService.KEEP_ALIVE_CORE_TAG;
  }

  private parse_peer_id(peer_id: string) {
    if (!this.peer_id_from_string) {
      return null;
    }

    try {
      return this.peer_id_from_string(peer_id);
    } catch {
      return null;
    }
  }

  private async get_peer_store_peer(peer_id_object: any) {
    if (!peer_id_object) {
      return null;
    }

    const peer_store = this.libp2p_node?.peerStore;

    if (!peer_store?.get) {
      return null;
    }

    try {
      return await peer_store.get(peer_id_object);
    } catch {
      return null;
    }
  }

  private is_peer_in_dial_queue(peer_id: string) {
    const dial_queue = this.libp2p_node?.getDialQueue?.();

    if (!Array.isArray(dial_queue)) {
      return false;
    }

    return dial_queue.some((pending_dial: any) => {
      const queued_peer_id =
        pending_dial?.peer?.id?.toString?.() ??
        pending_dial?.peerId?.toString?.() ??
        pending_dial?.id?.toString?.() ??
        null;

      return queued_peer_id === peer_id;
    });
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

  private async write_stream_payload(stream: any, payload: string) {
    const encoded_payload = new TextEncoder().encode(payload);

    if (stream?.sink) {
      await stream.sink(create_single_chunk_source(payload));
      return;
    }

    if (typeof stream?.send === 'function') {
      const can_continue = stream.send(encoded_payload);

      if (can_continue === false && typeof stream?.onDrain === 'function') {
        await stream.onDrain();
      }

      await stream.close?.();
      return;
    }

    throw new Error('Unsupported stream shape for write.');
  }

  private async read_stream_payload(stream: any) {
    if (stream == null) {
      return '';
    }

    const chunks: Uint8Array[] = [];

    if (stream?.source) {
      for await (const chunk of stream.source as AsyncIterable<StreamChunk>) {
        chunks.push(normalize_chunk(chunk));
      }
    } else if (typeof stream?.[Symbol.asyncIterator] === 'function') {
      for await (const chunk of stream as AsyncIterable<StreamChunk>) {
        chunks.push(normalize_chunk(chunk));
      }
    } else {
      return '';
    }

    if (chunks.length === 0) {
      return '';
    }

    return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))).toString(
      'utf8',
    );
  }

  private async cleanup_expired_presence_records() {
    await this.prisma_service.userPresence.deleteMany({
      where: {
        expires_at: {
          lte: new Date(),
        },
      },
    });
  }

  private async create_signed_inter_server_dm_event<Payload>(
    type: string,
    payload: Payload,
  ): Promise<SignedInterServerDmEvent<Payload>> {
    const unsigned_event = {
      event_id: randomUUID(),
      origin_peer_id: this.get_local_peer_id(),
      payload,
      server_public_key: (
        await this.server_identity_service.get_public_key()
      ).public_key,
      timestamp: new Date().toISOString(),
      type,
    };
    const { signature } = await create_inter_server_dm_signature({
      event: unsigned_event,
      sign_message: async (message) =>
        this.server_identity_service.sign_message(message),
    });

    return {
      ...unsigned_event,
      server_signature: signature,
    };
  }

  private async verify_and_extract_signed_inter_server_dm_event(
    event: SignedInterServerDmEvent<unknown> | null,
    source_peer_id: string,
    expected_type?: string,
  ) {
    if (!event || typeof event !== 'object') {
      this.trace_event(
        'p2p.inter_server_dm_event_rejected',
        'warn',
        {
          reason: 'Signed inter-server event payload is missing.',
        },
        source_peer_id,
      );
      return null;
    }

    if (!this.is_peer_directly_connected(source_peer_id)) {
      this.trace_event(
        'p2p.inter_server_dm_event_rejected',
        'warn',
        {
          reason: 'Source peer is not directly connected.',
        },
        source_peer_id,
      );
      return null;
    }

    if (expected_type && event.type !== expected_type) {
      this.trace_event(
        'p2p.inter_server_dm_event_rejected',
        'warn',
        {
          expected_type,
          received_type: event.type,
          reason: 'Unexpected inter-server event type.',
        },
        source_peer_id,
      );
      return null;
    }

    if (event.origin_peer_id !== source_peer_id) {
      this.trace_event(
        'p2p.inter_server_dm_event_rejected',
        'warn',
        {
          origin_peer_id: event.origin_peer_id,
          reason: 'Signed inter-server event origin peer mismatch.',
        },
        source_peer_id,
      );
      return null;
    }

    const is_signature_valid = await verify_inter_server_dm_signature(event);

    if (!is_signature_valid) {
      this.trace_event(
        'p2p.inter_server_dm_event_rejected',
        'warn',
        {
          reason: 'Inter-server event signature verification failed.',
          type: event.type,
        },
        source_peer_id,
      );
      return null;
    }

    return {
      event,
      payload: event.payload,
    };
  }

  private async request_signed_inter_server_dm_event<
    Payload,
    ResponsePayload,
  >(
    peer_id: string,
    protocol: string,
    request_type: string,
    payload: Payload,
    expected_response_type?: string,
  ) {
    const request_event = await this.create_signed_inter_server_dm_event(
      request_type,
      payload,
    );
    const response =
      await this.request_peer_protocol_json_for_peer_id(
        peer_id,
        protocol,
        request_event,
        this.inter_server_dm_config.p2p_request_timeout_ms,
      );
    const parsed_response =
      await this.verify_and_extract_signed_inter_server_dm_event(
        response as SignedInterServerDmEvent<unknown>,
        peer_id,
        expected_response_type,
      );

    if (!parsed_response) {
      throw new Error(
        `Peer ${peer_id} returned an invalid signed inter-server response for ${request_type}.`,
      );
    }

    return parsed_response.payload as ResponsePayload;
  }

  private parse_inter_server_dm_replicate_payload(
    value: unknown,
  ): InterServerDmReplicatePayload {
    if (typeof value !== 'object' || value == null) {
      throw new Error('Invalid dm.replicate payload.');
    }

    const payload = value as Record<string, unknown>;

    return {
      algorithm: String(payload.algorithm ?? ''),
      id: String(payload.id ?? ''),
      message: String(payload.message ?? ''),
      message_hash: String(payload.message_hash ?? ''),
      origin_server_peer_id: String(payload.origin_server_peer_id ?? ''),
      recipient_public_key: String(payload.recipient_public_key ?? ''),
      replica_peer_ids: Array.isArray(payload.replica_peer_ids)
        ? payload.replica_peer_ids
            .filter((item): item is string => typeof item === 'string')
            .map((item) => item.trim())
            .filter(Boolean)
        : [],
      send_time: String(payload.send_time ?? ''),
      sender_public_key: String(payload.sender_public_key ?? ''),
      sender_signature: String(payload.sender_signature ?? ''),
      sender_x25519_public_key: String(payload.sender_x25519_public_key ?? ''),
    };
  }

  private parse_inter_server_dm_presence_announce_payload(
    value: unknown,
  ): InterServerDmPresenceAnnouncePayload {
    if (typeof value !== 'object' || value == null) {
      throw new Error('Invalid dm.presence.announce payload.');
    }

    const payload = value as Record<string, unknown>;

    return {
      expires_at: String(payload.expires_at ?? ''),
      public_key: String(payload.public_key ?? ''),
      server_peer_id: String(payload.server_peer_id ?? ''),
    };
  }

  private parse_inter_server_dm_presence_query_payload(
    value: unknown,
  ): InterServerDmPresenceQueryPayload {
    if (typeof value !== 'object' || value == null) {
      throw new Error('Invalid dm.presence.query payload.');
    }

    const payload = value as Record<string, unknown>;

    return {
      public_key: String(payload.public_key ?? ''),
    };
  }

  private parse_inter_server_dm_pull_request_payload(
    value: unknown,
  ): InterServerDmPullRequestPayload {
    if (typeof value !== 'object' || value == null) {
      throw new Error('Invalid dm.pull.request payload.');
    }

    const payload = value as Record<string, unknown>;

    return {
      cursor:
        typeof payload.cursor === 'string'
          ? payload.cursor
          : payload.cursor === null
            ? null
            : undefined,
      limit: Math.max(
        1,
        Math.trunc(
          typeof payload.limit === 'number'
            ? payload.limit
            : this.inter_server_dm_config.pull_batch_size,
        ),
      ),
      public_key: String(payload.public_key ?? ''),
    };
  }

  private parse_inter_server_dm_delete_request_payload(
    value: unknown,
  ): InterServerDmDeleteRequestPayload {
    if (typeof value !== 'object' || value == null) {
      throw new Error('Invalid dm.delete.request payload.');
    }

    const payload = value as Record<string, unknown>;

    return {
      message_id: String(payload.message_id ?? ''),
      origin_server_peer_id: String(payload.origin_server_peer_id ?? ''),
      recipient_public_key: String(payload.recipient_public_key ?? ''),
      replica_peer_ids: Array.isArray(payload.replica_peer_ids)
        ? payload.replica_peer_ids
            .filter((item): item is string => typeof item === 'string')
            .map((item) => item.trim())
            .filter(Boolean)
        : [],
    };
  }

  private should_process_inter_server_dm_event(event_id: string, type: string) {
    this.cleanup_inter_server_dm_event_dedupe();

    const dedupe_key = `${type}:${event_id}`;

    if (this.inter_server_dm_event_dedupe.has(dedupe_key)) {
      return false;
    }

    this.inter_server_dm_event_dedupe.set(
      dedupe_key,
      Date.now() + this.p2p_config.sync_dedupe_ttl_seconds * 1000,
    );

    return true;
  }

  private cleanup_inter_server_dm_event_dedupe() {
    const now_ms = Date.now();

    for (const [
      dedupe_key,
      expires_at_ms,
    ] of this.inter_server_dm_event_dedupe.entries()) {
      if (expires_at_ms <= now_ms) {
        this.inter_server_dm_event_dedupe.delete(dedupe_key);
      }
    }
  }

  private async send_dm_delete_request_to_peer(
    peer_id: string,
    payload: InterServerDmDeleteRequestPayload,
  ) {
    if (peer_id === this.get_local_peer_id()) {
      try {
        const response = await this.inter_server_dm_callbacks?.on_delete_request(
          payload,
          peer_id,
        );
        return response?.deleted === true;
      } catch (error) {
        this.trace_event(
          'p2p.dm_delete_request_failed',
          'warn',
          {
            error: error instanceof Error ? error.message : String(error),
          },
          peer_id,
        );
        return false;
      }
    }

    try {
      const response = await this.request_signed_inter_server_dm_event<
        InterServerDmDeleteRequestPayload,
        InterServerDmDeleteAckPayload
      >(
        peer_id,
        PEER_DM_DELETE_PROTOCOL,
        'dm.delete.request',
        payload,
        'dm.delete.ack',
      );

      return response.deleted === true;
    } catch (error) {
      this.trace_event(
        'p2p.dm_delete_request_failed',
        'warn',
        {
          error: error instanceof Error ? error.message : String(error),
        },
        peer_id,
      );
      return false;
    }
  }

  private enqueue_dm_delete_retry(
    peer_id: string,
    payload: InterServerDmDeleteRequestPayload,
    attempts: number,
  ) {
    const queue_key = `${payload.message_id}:${peer_id}`;

    this.inter_server_dm_delete_retry_queue.set(queue_key, {
      attempts,
      next_attempt_at_ms:
        Date.now() + this.inter_server_dm_config.delete_retry_interval_ms,
      payload,
      peer_id,
    });
  }

  private async process_inter_server_dm_delete_retry_queue() {
    if (this.inter_server_dm_delete_retry_queue.size === 0) {
      return;
    }

    const now_ms = Date.now();

    for (const [queue_key, item] of this.inter_server_dm_delete_retry_queue) {
      if (item.next_attempt_at_ms > now_ms) {
        continue;
      }

      const success = await this.send_dm_delete_request_to_peer(
        item.peer_id,
        item.payload,
      );

      if (success) {
        this.inter_server_dm_delete_retry_queue.delete(queue_key);
        continue;
      }

      const next_attempts = item.attempts + 1;

      if (next_attempts >= this.inter_server_dm_config.delete_max_retries) {
        this.inter_server_dm_delete_retry_queue.delete(queue_key);
        this.trace_event(
          'p2p.dm_delete_retry_exhausted',
          'warn',
          {
            attempts: next_attempts,
            message_id: item.payload.message_id,
          },
          item.peer_id,
        );
        continue;
      }

      this.inter_server_dm_delete_retry_queue.set(queue_key, {
        ...item,
        attempts: next_attempts,
        next_attempt_at_ms:
          now_ms + this.inter_server_dm_config.delete_retry_interval_ms,
      });
    }
  }

  private async get_closest_peer_ids_for_key(key: string, count: number) {
    const local_peer_id = this.get_local_peer_id();
    const candidate_peer_ids = new Set<string>();

    for (const peer of this.get_connected_peers()) {
      if (peer.peer_id && peer.peer_id !== local_peer_id) {
        candidate_peer_ids.add(peer.peer_id);
      }
    }

    const server_nodes = await this.prisma_service.serverNode.findMany({
      where: {
        is_active: true,
      },
      orderBy: {
        last_seen_at: 'desc',
      },
      take: Math.max(50, count * 6),
    });

    for (const server_node of server_nodes) {
      if (server_node.peer_id && server_node.peer_id !== local_peer_id) {
        candidate_peer_ids.add(server_node.peer_id);
      }
    }

    const dht_peer_ids = await this.collect_dht_closest_peer_ids(key, count);

    for (const peer_id of dht_peer_ids) {
      if (peer_id && peer_id !== local_peer_id) {
        candidate_peer_ids.add(peer_id);
      }
    }

    return this.sort_peer_ids_by_key_distance(Array.from(candidate_peer_ids), key);
  }

  private async collect_dht_closest_peer_ids(key: string, count: number) {
    const dht = this.libp2p_node?.services?.dht;

    if (!dht || typeof dht.getClosestPeers !== 'function') {
      return [] as string[];
    }

    const key_hash = createHash('sha256').update(key).digest();
    const peer_ids = new Set<string>();

    try {
      for await (const event of dht.getClosestPeers(key_hash)) {
        const event_name = (event as any)?.name;

        if (event_name === 'FINAL_PEER') {
          const peer_id = (event as any)?.peer?.id?.toString?.();

          if (peer_id) {
            peer_ids.add(peer_id);
          }
        }

        if (event_name === 'PEER_RESPONSE') {
          const closer = (event as any)?.closer;

          if (Array.isArray(closer)) {
            for (const peer of closer) {
              const peer_id = peer?.id?.toString?.();

              if (peer_id) {
                peer_ids.add(peer_id);
              }
            }
          }
        }

        if (peer_ids.size >= Math.max(count * 3, count + 3)) {
          break;
        }
      }
    } catch (error) {
      this.trace_event('p2p.dm_dht_lookup_failed', 'warn', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    return Array.from(peer_ids);
  }

  private sort_peer_ids_by_key_distance(peer_ids: string[], key: string) {
    const key_hash = createHash('sha256').update(key).digest();

    return [...peer_ids]
      .map((peer_id) => ({
        distance_hex: this.create_xor_distance_hex(
          key_hash,
          createHash('sha256').update(peer_id).digest(),
        ),
        peer_id,
      }))
      .sort((left, right) => {
        if (left.distance_hex !== right.distance_hex) {
          return left.distance_hex.localeCompare(right.distance_hex);
        }

        return left.peer_id.localeCompare(right.peer_id);
      })
      .map((item) => item.peer_id);
  }

  private create_xor_distance_hex(left: Buffer, right: Buffer) {
    const max_length = Math.max(left.length, right.length);
    const out = Buffer.alloc(max_length);

    for (let index = 0; index < max_length; index += 1) {
      const left_byte = left[index] ?? 0;
      const right_byte = right[index] ?? 0;
      out[index] = left_byte ^ right_byte;
    }

    return out.toString('hex');
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
