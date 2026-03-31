import {
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { get_realtime_config } from '../../../config/realtime.config';
import { ServerIdentityService } from '../../server_identity/server_identity.service';
import { AuthChallengeRecord, AuthConnectPayload } from '../types/realtime.types';
import { create_auth_challenge_key } from '../utils/realtime_keys.util';
import {
  create_auth_challenge_message,
  verify_message_signature,
} from '../utils/realtime_signature.util';
import { randomBytes, randomUUID } from 'node:crypto';
import { RealtimeRedisService } from './realtime_redis.service';

@Injectable()
export class RealtimeChallengeService {
  private readonly realtime_config = get_realtime_config();

  constructor(
    private readonly realtime_redis_service: RealtimeRedisService,
    private readonly server_identity_service: ServerIdentityService,
  ) {}

  async create_challenge(public_key: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const challenge_id = randomUUID();
    const nonce = `0x${randomBytes(32).toString('hex')}`;
    const expires_at = new Date(
      Date.now() + this.realtime_config.challenge_ttl_seconds * 1000,
    ).toISOString();
    const created_at = new Date().toISOString();
    const challenge_record: AuthChallengeRecord = {
      challenge_id,
      created_at,
      expires_at,
      nonce,
      public_key,
    };
    const challenge_message = create_auth_challenge_message(
      challenge_record.challenge_id,
      challenge_record.public_key,
      challenge_record.nonce,
      challenge_record.expires_at,
    );
    const server_public_key_response =
      await this.server_identity_service.get_public_key();
    const server_signature = await this.server_identity_service.sign_message(
      challenge_message,
    );

    await command_client.set(
      create_auth_challenge_key(challenge_id),
      JSON.stringify(challenge_record),
      'EX',
      this.realtime_config.challenge_ttl_seconds,
    );

    return {
      algorithm: 'ed25519' as const,
      challenge_id,
      expires_at,
      nonce,
      server_public_key: server_public_key_response.public_key,
      server_signature,
    };
  }

  async consume_and_verify_challenge(payload: AuthConnectPayload) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const raw_record = await command_client.getdel(
      create_auth_challenge_key(payload.challenge_id),
    );

    if (!raw_record) {
      throw new UnauthorizedException(
        'Challenge is invalid, expired, or already used.',
      );
    }

    const challenge_record = JSON.parse(raw_record) as AuthChallengeRecord;

    if (challenge_record.public_key !== payload.public_key) {
      throw new UnauthorizedException('Challenge does not match this public key.');
    }

    if (new Date(challenge_record.expires_at).getTime() < Date.now()) {
      throw new UnauthorizedException('Challenge has expired.');
    }

    const challenge_message = create_auth_challenge_message(
      challenge_record.challenge_id,
      challenge_record.public_key,
      challenge_record.nonce,
      challenge_record.expires_at,
    );
    const is_valid_signature = await verify_message_signature(
      payload.public_key,
      payload.signature,
      challenge_message,
    );

    if (!is_valid_signature) {
      throw new UnauthorizedException('Challenge signature verification failed.');
    }

    return challenge_record;
  }
}
