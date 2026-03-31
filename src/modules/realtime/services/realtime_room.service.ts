import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { REALTIME_NODE_ID } from '../constants/realtime.constants';
import { RoomControlMessage } from '../types/realtime.types';
import { create_session_rooms_key } from '../utils/realtime_keys.util';
import { RealtimeConnectionService } from './realtime_connection.service';
import { RealtimeRedisService } from './realtime_redis.service';
import { RealtimeSessionService } from './realtime_session.service';

@Injectable()
export class RealtimeRoomService implements OnModuleInit, OnModuleDestroy {
  private readonly local_room_sessions = new Map<string, Set<string>>();
  private readonly local_session_rooms = new Map<string, Set<string>>();
  private unsubscribe_room_control: (() => void) | null = null;

  constructor(
    @Inject(REALTIME_NODE_ID) private readonly node_id: string,
    private readonly realtime_connection_service: RealtimeConnectionService,
    private readonly realtime_redis_service: RealtimeRedisService,
    private readonly realtime_session_service: RealtimeSessionService,
  ) {}

  onModuleInit() {
    this.unsubscribe_room_control =
      this.realtime_redis_service.register_room_control_handler(async (message) => {
        await this.handle_room_control_message(message);
      });
  }

  onModuleDestroy() {
    this.unsubscribe_room_control?.();
  }

  async replace_session_rooms(
    session_id: string,
    rooms: string[],
    should_publish = true,
  ) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const session_rooms_key = create_session_rooms_key(session_id);
    const unique_rooms = [...new Set(rooms)];
    const multi = command_client.multi();

    multi.del(session_rooms_key);

    if (unique_rooms.length > 0) {
      multi.sadd(session_rooms_key, ...unique_rooms);
      multi.pexpire(
        session_rooms_key,
        this.realtime_session_service.get_session_ttl_ms(),
      );
    }

    await multi.exec();
    this.sync_local_rooms(session_id, unique_rooms);

    if (should_publish) {
      await this.realtime_redis_service.publish_room_control({
        action: 'replace',
        rooms: unique_rooms,
        session_id,
      });
    }

    return unique_rooms;
  }

  async add_room_to_session(
    session_id: string,
    room_name: string,
    should_publish = true,
  ) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const session_rooms_key = create_session_rooms_key(session_id);

    await command_client
      .multi()
      .sadd(session_rooms_key, room_name)
      .pexpire(
        session_rooms_key,
        this.realtime_session_service.get_session_ttl_ms(),
      )
      .exec();
    this.add_local_room_membership(session_id, room_name);

    if (should_publish) {
      await this.realtime_redis_service.publish_room_control({
        action: 'join',
        room_name,
        session_id,
      });
    }
  }

  async remove_room_from_session(
    session_id: string,
    room_name: string,
    should_publish = true,
  ) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.srem(create_session_rooms_key(session_id), room_name);
    this.remove_local_room_membership(session_id, room_name);

    if (should_publish) {
      await this.realtime_redis_service.publish_room_control({
        action: 'leave',
        room_name,
        session_id,
      });
    }
  }

  async restore_session_rooms(session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();
    const rooms = await command_client.smembers(create_session_rooms_key(session_id));

    this.sync_local_rooms(session_id, rooms);

    return rooms;
  }

  async touch_session_rooms(session_id: string) {
    const command_client = this.realtime_redis_service.get_commands_client();

    await command_client.pexpire(
      create_session_rooms_key(session_id),
      this.realtime_session_service.get_session_ttl_ms(),
    );
  }

  clear_local_memberships(session_id: string) {
    const rooms = this.local_session_rooms.get(session_id);

    if (!rooms) {
      return;
    }

    for (const room_name of rooms) {
      const session_ids = this.local_room_sessions.get(room_name);

      if (!session_ids) {
        continue;
      }

      session_ids.delete(session_id);

      if (session_ids.size === 0) {
        this.local_room_sessions.delete(room_name);
      }
    }

    this.local_session_rooms.delete(session_id);
  }

  get_local_session_ids(room_name: string) {
    return [...(this.local_room_sessions.get(room_name) ?? new Set<string>())];
  }

  get_local_rooms(session_id: string) {
    return [...(this.local_session_rooms.get(session_id) ?? new Set<string>())];
  }

  private async handle_room_control_message(message: RoomControlMessage) {
    if (!this.realtime_connection_service.has_local_session(message.session_id)) {
      return;
    }

    if (message.action === 'replace') {
      this.sync_local_rooms(message.session_id, message.rooms ?? []);
      return;
    }

    if (!message.room_name) {
      return;
    }

    if (message.action === 'join') {
      this.add_local_room_membership(message.session_id, message.room_name);
      return;
    }

    if (message.action === 'leave') {
      this.remove_local_room_membership(message.session_id, message.room_name);
    }
  }

  private sync_local_rooms(session_id: string, rooms: string[]) {
    this.clear_local_memberships(session_id);

    for (const room_name of rooms) {
      this.add_local_room_membership(session_id, room_name);
    }
  }

  private add_local_room_membership(session_id: string, room_name: string) {
    if (!this.realtime_connection_service.has_local_session(session_id)) {
      return;
    }

    const room_sessions = this.local_room_sessions.get(room_name) ?? new Set<string>();
    const session_rooms = this.local_session_rooms.get(session_id) ?? new Set<string>();

    room_sessions.add(session_id);
    session_rooms.add(room_name);
    this.local_room_sessions.set(room_name, room_sessions);
    this.local_session_rooms.set(session_id, session_rooms);
  }

  private remove_local_room_membership(session_id: string, room_name: string) {
    const room_sessions = this.local_room_sessions.get(room_name);

    if (room_sessions) {
      room_sessions.delete(session_id);

      if (room_sessions.size === 0) {
        this.local_room_sessions.delete(room_name);
      }
    }

    const session_rooms = this.local_session_rooms.get(session_id);

    if (!session_rooms) {
      return;
    }

    session_rooms.delete(room_name);

    if (session_rooms.size === 0) {
      this.local_session_rooms.delete(session_id);
      return;
    }

    this.local_session_rooms.set(session_id, session_rooms);
  }
}
