import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { get_p2p_config } from 'src/config/p2p.config';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PEER_HELLO_PROTOCOL } from './constants/peer_protocol.constants';

type ConnectedPeerSnapshot = {
  peer_id: string;
  remote_address: string | null;
};

@Injectable()
export class PeerNetworkService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PeerNetworkService.name);
  private readonly p2p_config = get_p2p_config();
  private libp2p_node: any = null;

  constructor(private readonly network_trace_service: NetworkTraceService) {}

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

    for (const bootstrap_address of this.p2p_config.bootstrap) {
      const normalized_address =
        this.normalize_bootstrap_address(bootstrap_address);
      const peer_id = this.extract_peer_id_from_multiaddr(normalized_address);

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
  }

  async onModuleDestroy() {
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
        this.trace_event('p2p.hello_received', 'info', undefined, remote_peer_id);

        if (stream?.sink != null) {
          await stream.sink(
            (async function* () {
              yield new TextEncoder().encode('hello-ack');
            })(),
          );
          this.trace_event('p2p.hello_ack_sent', 'info', undefined, remote_peer_id);
        }
      },
    );

    this.libp2p_node.addEventListener?.('peer:connect', (event: any) => {
      const peer_id = event?.detail?.toString?.() ?? 'unknown_peer';
      this.logger.log(`Peer connected: ${peer_id}`);
      this.trace_event('p2p.peer_connected', 'info', undefined, peer_id);
    });

    this.libp2p_node.addEventListener?.('peer:disconnect', (event: any) => {
      const peer_id = event?.detail?.toString?.() ?? 'unknown_peer';
      this.logger.warn(`Peer disconnected: ${peer_id}`);
      this.trace_event('p2p.peer_disconnected', 'warn', undefined, peer_id);
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
      this.libp2p_node
        ?.getMultiaddrs?.()
        ?.map((address: any) => {
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

