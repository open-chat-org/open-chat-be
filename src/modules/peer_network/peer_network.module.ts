import { Module } from '@nestjs/common';
import { PeerNetworkController } from './peer_network.controller';
import { PeerNetworkService } from './peer_network.service';

@Module({
  controllers: [PeerNetworkController],
  providers: [PeerNetworkService],
  exports: [PeerNetworkService],
})
export class PeerNetworkModule {}
