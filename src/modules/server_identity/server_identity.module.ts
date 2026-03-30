import { Module } from '@nestjs/common';
import { ServerIdentityController } from './server_identity.controller';
import { ServerIdentityService } from './server_identity.service';

@Module({
  controllers: [ServerIdentityController],
  providers: [ServerIdentityService],
  exports: [ServerIdentityService],
})
export class ServerIdentityModule {}
