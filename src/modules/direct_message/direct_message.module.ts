import { forwardRef, Module } from '@nestjs/common';
import { PeerNetworkModule } from '../peer_network/peer_network.module';
import { PrismaModule } from '../prisma/prisma.module';
import { RealtimeModule } from '../realtime/realtime.module';
import { DirectMessageService } from './direct_message.service';

@Module({
  imports: [PrismaModule, forwardRef(() => RealtimeModule), PeerNetworkModule],
  providers: [DirectMessageService],
  exports: [DirectMessageService],
})
export class DirectMessageModule {}
