import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { ServerIdentityModule } from '../server_identity/server_identity.module';
import { PeerNetworkController } from './peer_network.controller';
import { PeerNetworkService } from './peer_network.service';

@Module({
  imports: [PrismaModule, ServerIdentityModule],
  controllers: [PeerNetworkController],
  providers: [PeerNetworkService],
  exports: [PeerNetworkService],
})
export class PeerNetworkModule {}
