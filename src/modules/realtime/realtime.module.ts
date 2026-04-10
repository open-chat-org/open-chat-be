import { forwardRef, Module } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { DirectMessageModule } from '../direct_message/direct_message.module';
import { ServerIdentityModule } from '../server_identity/server_identity.module';
import { REALTIME_NODE_ID } from './constants/realtime.constants';
import { RealtimeController } from './realtime.controller';
import { RealtimeGateway } from './realtime.gateway';
import { RealtimeChallengeService } from './services/realtime_challenge.service';
import { RealtimeConnectionService } from './services/realtime_connection.service';
import { RealtimeDeliveryService } from './services/realtime_delivery.service';
import { RealtimeFanoutService } from './services/realtime_fanout.service';
import { RealtimeRedisService } from './services/realtime_redis.service';
import { RealtimeRoomService } from './services/realtime_room.service';
import { RealtimeService } from './services/realtime.service';
import { RealtimeSessionService } from './services/realtime_session.service';

@Module({
  imports: [ServerIdentityModule, forwardRef(() => DirectMessageModule)],
  controllers: [RealtimeController],
  providers: [
    {
      provide: REALTIME_NODE_ID,
      useFactory: () => randomUUID(),
    },
    RealtimeGateway,
    RealtimeChallengeService,
    RealtimeConnectionService,
    RealtimeDeliveryService,
    RealtimeFanoutService,
    RealtimeRedisService,
    RealtimeRoomService,
    RealtimeService,
    RealtimeSessionService,
  ],
  exports: [RealtimeService],
})
export class RealtimeModule {}
