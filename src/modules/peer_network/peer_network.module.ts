import { Module } from '@nestjs/common';
import { PeerNetworkService } from './peer_network.service';

@Module({
  providers: [PeerNetworkService]
})
export class PeerNetworkModule {}
