import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { get_p2p_config } from 'src/config/p2p.config';

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

        // this.register_protocol_handlers();
        // this.log_node_identity();

        for (const addr of config.bootstrap) {
            try {
              await this.libp2p_node.dial(multiaddr(addr));
              this.logger.log(`Dialed bootstrap peer: ${addr}`);
            } catch {
              this.logger.warn(`Bootstrap dial failed: ${addr}`);
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
}
