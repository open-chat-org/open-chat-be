import { Controller, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { PublicKeyHeader } from '../../common/decorators/public_key_header.decorator';
import { PublicKeyHeaderPipe } from '../../common/pipes/public_key_header.pipe';
import { RealtimeChallengeService } from './services/realtime_challenge.service';

@Controller('realtime')
export class RealtimeController {
  constructor(
    private readonly realtime_challenge_service: RealtimeChallengeService,
  ) {}

  @Post('challenge')
  @HttpCode(HttpStatus.CREATED)
  async create_challenge(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
  ) {
    return this.realtime_challenge_service.create_challenge(public_key);
  }
}
