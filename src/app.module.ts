import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './modules/prisma/prisma.module';
import { RealtimeModule } from './modules/realtime/realtime.module';
import { ServerIdentityModule } from './modules/server_identity/server_identity.module';
import { UserModule } from './modules/user/user.module';
import { PeerNetworkModule } from './modules/peer_network/peer_network.module';
import { NetworkTraceModule } from './modules/network_trace/network_trace.module';

@Module({
  imports: [
    PrismaModule,
    NetworkTraceModule,
    ServerIdentityModule,
    UserModule,
    RealtimeModule,
    PeerNetworkModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
