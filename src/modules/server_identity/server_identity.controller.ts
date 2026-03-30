import { Controller, Get } from '@nestjs/common';
import { ServerIdentityService } from './server_identity.service';

@Controller('server')
export class ServerIdentityController {
  constructor(
    private readonly server_identity_service: ServerIdentityService,
  ) {}

  @Get('public_key')
  async get_public_key() {
    return this.server_identity_service.get_public_key();
  }
}
