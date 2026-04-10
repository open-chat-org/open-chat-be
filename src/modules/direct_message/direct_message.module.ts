import { forwardRef, Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { RealtimeModule } from '../realtime/realtime.module';
import { DirectMessageService } from './direct_message.service';

@Module({
  imports: [PrismaModule, forwardRef(() => RealtimeModule)],
  providers: [DirectMessageService],
  exports: [DirectMessageService],
})
export class DirectMessageModule {}
