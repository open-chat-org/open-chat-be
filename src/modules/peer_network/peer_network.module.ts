import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { ServerIdentityModule } from '../server_identity/server_identity.module';
import { PeerNetworkController } from './peer_network.controller';
import { PeerNetworkService } from './peer_network.service';
import { DmDeleteGossipService } from './sync/services/dm_delete_gossip.service';
import { StartupSyncService } from './sync/services/startup_sync.service';
import { SyncProgressLoggerService } from './sync/services/sync_progress_logger.service';
import { TableSyncRunnerService } from './sync/services/table_sync_runner.service';
import { ValidatorDiscoveryService } from './sync/services/validator_discovery.service';
import { VoteVerificationService } from './sync/services/vote_verification.service';

@Module({
  imports: [PrismaModule, ServerIdentityModule],
  controllers: [PeerNetworkController],
  providers: [
    DmDeleteGossipService,
    PeerNetworkService,
    StartupSyncService,
    SyncProgressLoggerService,
    TableSyncRunnerService,
    ValidatorDiscoveryService,
    VoteVerificationService,
  ],
  exports: [DmDeleteGossipService, PeerNetworkService],
})
export class PeerNetworkModule {}
