import { Inject, Injectable } from '@nestjs/common';
import { get_realtime_config } from '../../../config/realtime.config';
import { REALTIME_NODE_ID } from '../constants/realtime.constants';
import { RealtimeSessionRecord } from '../types/realtime.types';
import {
  create_session_key,
  create_user_sessions_key,
} from '../utils/realtime_keys.util';
import { RealtimeRedisService } from './realtime_redis.service';
import { randomUUID } from 'node:crypto';

@Injectable()
export class RealtimeSessionService {
  private readonly realtime_config = get_realtime_config();

  constructor(
    @Inject(REALTIME_NODE_ID) private readonly node_id: string,
    private readonly realtime_redis_service: RealtimeRedisService,
  ) {}

  async create_or_restore_session(
    public_key: string,
    last_session_id?: string,
  ) {
    if (last_session_id) {
      const existing_session = await this.get_session(last_session_id);

      if (
        existing_session &&
        existing_session.public_key === public_key &&
        new Date(existing_session.reconnect_expires_at).getTime() >= Date.now()
      ) {
        const restored_session: RealtimeSessionRecord = {
          ...existing_session,
          connected_at: new Date().toISOString(),
          last_seen_at: new Date().toISOString(),
          node_id: this.node_id,
          reconnect_expires_at: this.build_reconnect_expiry(),
          status: 'active',
        };

        await this.persist_session(restored_session);
        await this.add_active_session(restored_session.public_key, last_session_id);

        return {
          restored: true,
          session: restored_session,
        };
      }
    }

    const created_session: RealtimeSessionRecord = {
      connected_at: new Date().toISOString(),
      last_seen_at: new Date().toISOString(),
      node_id: this.node_id,
      public_key,
      reconnect_expires_at: this.build_reconnect_expiry(),
      session_id: randomUUID(),
      status: 'active',
    };

    await this.persist_session(created_session);
    await this.add_active_session(public_key, created_session.session_id);

    return {
      restored: false,
      session: created_session,
    };
  }

  async get_session(session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const raw_session = await command_client.get(create_session_key(session_id));

    if (!raw_session) {
      return null;
    }

    return JSON.parse(raw_session) as RealtimeSessionRecord;
  }

  async touch_session(session_id: string) {
    const existing_session = await this.get_session(session_id);

    if (!existing_session) {
      return null;
    }

    const touched_session: RealtimeSessionRecord = {
      ...existing_session,
      last_seen_at: new Date().toISOString(),
      reconnect_expires_at: this.build_reconnect_expiry(),
    };

    await this.persist_session(touched_session);

    if (touched_session.status === 'active') {
      await this.add_active_session(touched_session.public_key, session_id);
    }

    return touched_session;
  }

  async mark_session_disconnected(session_id: string) {
    const existing_session = await this.get_session(session_id);

    if (!existing_session) {
      return null;
    }

    if (existing_session.node_id !== this.node_id && existing_session.status === 'active') {
      return existing_session;
    }

    const disconnected_session: RealtimeSessionRecord = {
      ...existing_session,
      last_seen_at: new Date().toISOString(),
      reconnect_expires_at: this.build_reconnect_expiry(),
      status: 'disconnected',
    };

    await this.persist_session(disconnected_session);
    await this.remove_active_session(existing_session.public_key, session_id);

    return disconnected_session;
  }

  async get_active_session_ids(public_key: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const user_sessions_key = create_user_sessions_key(public_key);
    const session_ids = await command_client.smembers(user_sessions_key);

    if (session_ids.length === 0) {
      return [];
    }

    const sessions = await Promise.all(
      session_ids.map(async (session_id) => ({
        session: await this.get_session(session_id),
        session_id,
      })),
    );
    const active_session_ids = sessions
      .filter(
        ({ session }) =>
          session?.public_key === public_key && session.status === 'active',
      )
      .map(({ session_id }) => session_id);
    const stale_session_ids = session_ids.filter(
      (session_id) => !active_session_ids.includes(session_id),
    );

    if (stale_session_ids.length > 0) {
      await command_client.srem(user_sessions_key, ...stale_session_ids);
    }

    return active_session_ids;
  }

  get_session_ttl_ms() {
    return this.realtime_config.reconnect_grace_ms + 30_000;
  }

  private build_reconnect_expiry() {
    return new Date(
      Date.now() + this.realtime_config.reconnect_grace_ms,
    ).toISOString();
  }

  private async persist_session(session: RealtimeSessionRecord) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.set(
      create_session_key(session.session_id),
      JSON.stringify(session),
      'PX',
      this.get_session_ttl_ms(),
    );
  }

  private async add_active_session(public_key: string, session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.sadd(create_user_sessions_key(public_key), session_id);
  }

  private async remove_active_session(public_key: string, session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.srem(create_user_sessions_key(public_key), session_id);
  }
}
