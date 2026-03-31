import { Injectable } from '@nestjs/common';
import { RealtimeFanoutService } from './realtime_fanout.service';
import { RealtimeRoomService } from './realtime_room.service';
import { RealtimeSessionService } from './realtime_session.service';
import { create_chat_room_name, create_user_room_name } from '../utils/realtime_room.util';
import { ReliableEnvelopeInput } from '../types/realtime.types';

@Injectable()
export class RealtimeService {
  constructor(
    private readonly realtime_fanout_service: RealtimeFanoutService,
    private readonly realtime_room_service: RealtimeRoomService,
    private readonly realtime_session_service: RealtimeSessionService,
  ) {}

  async emit_to_user(public_key: string, envelope: ReliableEnvelopeInput) {
    return this.realtime_fanout_service.emit_to_room(
      create_user_room_name(public_key),
      envelope,
    );
  }

  async emit_to_chat_room(room_id: string, envelope: ReliableEnvelopeInput) {
    return this.realtime_fanout_service.emit_to_room(
      create_chat_room_name(room_id),
      {
        ...envelope,
        room_id,
      },
    );
  }

  async join_chat_room(public_key: string, room_id: string) {
    const room_name = create_chat_room_name(room_id);
    const session_ids =
      await this.realtime_session_service.get_active_session_ids(public_key);

    await Promise.all(
      session_ids.map(async (session_id) => {
        await this.realtime_room_service.add_room_to_session(session_id, room_name);
      }),
    );
  }

  async leave_chat_room(public_key: string, room_id: string) {
    const room_name = create_chat_room_name(room_id);
    const session_ids =
      await this.realtime_session_service.get_active_session_ids(public_key);

    await Promise.all(
      session_ids.map(async (session_id) => {
        await this.realtime_room_service.remove_room_from_session(
          session_id,
          room_name,
        );
      }),
    );
  }
}
