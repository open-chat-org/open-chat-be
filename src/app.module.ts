import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './modules/prisma/prisma.module';
import { RealtimeModule } from './modules/realtime/realtime.module';
import { ServerIdentityModule } from './modules/server_identity/server_identity.module';
import { UserModule } from './modules/user/user.module';

@Module({
  imports: [PrismaModule, ServerIdentityModule, UserModule, RealtimeModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
