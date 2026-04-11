import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { get_p2p_config } from 'src/config/p2p.config';
import { PEER_HELLO_PROTOCOL } from './constants/peer_protocol.constants';

@Injectable()
export class PeerNetworkService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(PeerNetworkService.name);
    private libp2p_node: any = null;

    async onModuleInit() {
        const config = get_p2p_config();
        if (!config.enabled) {
            this.logger.log('P2P is disabled');
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
            {multiaddr}
        ] =
            await Promise.all([
                import('libp2p'),
                import('@libp2p/tcp'),
                import('@chainsafe/libp2p-quic'),
                import('@chainsafe/libp2p-noise'),
                import('@libp2p/yamux'),
                import('@libp2p/kad-dht'),
                import('@libp2p/ping'),
                import('@libp2p/identify'),
                import('@multiformats/multiaddr')
            ]);

        this.libp2p_node = await createLibp2p({
            addresses: {
                listen: [config.listen],
            },
            transports: [tcp(), quic()],
            connectionEncrypters: [noise()],
            streamMuxers: [yamux()],
            connectionManager: {
                maxConnections: Math.max(32, config.target_peers * 2),
                maxParallelDials: Math.max(25, config.target_peers),
                maxIncomingPendingConnections: 20,
            },
            services: {
                identify: identify(),
                ping: ping(),
                dht: kadDHT({
                    kBucketSize: config.k_bucket_size,
                    clientMode: false,
                    alpha: config.alpha,
                }),
            },
        });

        this.logger.log(`P2P node started on ${config.listen}`);

        this.register_protocol_handlers();
        this.log_node_identity();

        for (const addr of config.bootstrap) {
            try {
              const normalized_addr = this.normalize_bootstrap_address(addr);
              await this.libp2p_node.dial(multiaddr(normalized_addr));
              this.logger.log(`Dialed bootstrap peer: ${normalized_addr}`);
            } catch (error) {
              const error_message = error instanceof Error ? error.message : String(error);
              this.logger.warn(`Bootstrap dial failed: ${addr} (${error_message})`);
            }
          }

    }

    async onModuleDestroy() {
        if (this.libp2p_node == null) {
            return;
        }

        await this.libp2p_node.stop();
        this.logger.log('P2P node stopped');
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

                if (stream?.sink != null) {
                    await stream.sink(
                        (async function* () {
                            yield new TextEncoder().encode('hello-ack');
                        })(),
                    );
                }
            },
        );

        this.libp2p_node.addEventListener?.('peer:connect', (event: any) => {
            const peer_id = event?.detail?.toString?.() ?? 'unknown_peer';
            this.logger.log(`Peer connected: ${peer_id}`);
        });

        this.libp2p_node.addEventListener?.('peer:disconnect', (event: any) => {
            const peer_id = event?.detail?.toString?.() ?? 'unknown_peer';
            this.logger.warn(`Peer disconnected: ${peer_id}`);
        });
    }

    private log_node_identity() {
        if (this.libp2p_node == null) {
            return;
        }

        const peer_id = this.libp2p_node.peerId?.toString?.() ?? 'unknown_peer';
        const addresses =
            this.libp2p_node
                .getMultiaddrs?.()
                ?.map((address: any) => {
                    const address_text = address.toString();
                    const peer_suffix = `/p2p/${peer_id}`;
                    return address_text.endsWith(peer_suffix)
                        ? address_text
                        : `${address_text}${peer_suffix}`;
                }) ?? [];

        this.logger.log(`P2P node identity: ${peer_id}`);
        for (const address of addresses) {
            this.logger.log(`P2P listen address: ${address}`);
        }
    }

    private normalize_bootstrap_address(address: string): string {
        let normalized_address = address.trim();
        if (!normalized_address.startsWith('/')) {
            normalized_address = `/${normalized_address}`;
        }

        // If someone accidentally pastes ".../p2p/<id>/p2p/<id>", collapse it.
        normalized_address = normalized_address.replace(
            /\/p2p\/([^/]+)\/p2p\/\1$/,
            '/p2p/$1',
        );

        return normalized_address;
    }
}
