import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets';
import {
  BadRequestException,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { get_realtime_config } from '../../config/realtime.config';
import WebSocket, { Server } from 'ws';
import { randomUUID } from 'node:crypto';
import { DirectMessageService } from '../direct_message/direct_message.service';
import { RealtimeChallengeService } from './services/realtime_challenge.service';
import { RealtimeConnectionService } from './services/realtime_connection.service';
import { RealtimeDeliveryService } from './services/realtime_delivery.service';
import { RealtimeRoomService } from './services/realtime_room.service';
import { RealtimeSessionService } from './services/realtime_session.service';
import { create_user_room_name } from './utils/realtime_room.util';
import { parse_auth_connect_payload } from './utils/realtime_signature.util';
import { DeliveryAckPayload } from './types/realtime.types';

type AuthenticatedSocketState = {
  auth_timeout: NodeJS.Timeout | null;
  authenticated: true;
  awaiting_pong: boolean;
  missed_pongs: number;
  public_key: string;
  session_id: string;
  socket_id: string;
};

@WebSocketGateway({
  path: get_realtime_config().ws_path,
})
export class RealtimeGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(RealtimeGateway.name);

  constructor(
    private readonly direct_message_service: DirectMessageService,
    private readonly realtime_challenge_service: RealtimeChallengeService,
    private readonly realtime_connection_service: RealtimeConnectionService,
    private readonly realtime_delivery_service: RealtimeDeliveryService,
    private readonly realtime_room_service: RealtimeRoomService,
    private readonly realtime_session_service: RealtimeSessionService,
  ) {}

  afterInit(_server: Server) {
    this.realtime_connection_service.start_heartbeat((client) => {
      this.logger.warn('Realtime client missed heartbeat window. Terminating.');
      client.terminate();
    });
  }

  handleConnection(client: WebSocket) {
    this.realtime_connection_service.register_client(
      client,
      randomUUID(),
      () => {
        this.send_system_error(client, 'Authentication timed out.');
        client.close(4401, 'Authentication timed out');
      },
    );
    client.on('pong', () => {
      void this.handle_pong(client);
    });
    client.on('error', (error) => {
      this.logger.warn(`Realtime socket error: ${String(error)}`);
    });
  }

  async handleDisconnect(client: WebSocket) {
    const state = this.realtime_connection_service.remove_client(client);

    if (!state?.session_id) {
      return;
    }

    if (this.realtime_connection_service.has_local_session(state.session_id)) {
      return;
    }

    this.realtime_room_service.clear_local_memberships(state.session_id);
    await Promise.all([
      this.realtime_delivery_service.touch_pending_ttl(state.session_id),
      this.realtime_room_service.touch_session_rooms(state.session_id),
      this.realtime_session_service.mark_session_disconnected(state.session_id),
    ]);
  }

  @SubscribeMessage('auth.connect')
  async handle_auth_connect(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() body: unknown,
  ) {
    const existing_state =
      this.realtime_connection_service.get_authenticated_state(client);

    if (existing_state) {
      this.send_system_error(client, 'Socket is already authenticated.');
      return;
    }

    try {
      const payload = parse_auth_connect_payload(body);
      await this.realtime_challenge_service.consume_and_verify_challenge(payload);

      if (payload.last_session_id) {
        this.realtime_connection_service.close_session_clients(
          payload.last_session_id,
          4001,
          'Session resumed by a new connection',
        );
      }

      const session_result =
        await this.realtime_session_service.create_or_restore_session(
          payload.public_key,
          payload.last_session_id,
        );
      const session_id = session_result.session.session_id;

      this.realtime_connection_service.mark_authenticated(
        client,
        session_id,
        payload.public_key,
      );

      const restored_rooms = session_result.restored
        ? await this.realtime_room_service.restore_session_rooms(session_id)
        : [];
      const default_user_room = create_user_room_name(payload.public_key);
      const rooms =
        restored_rooms.length > 0
          ? Array.from(new Set([default_user_room, ...restored_rooms]))
          : [default_user_room];

      await this.realtime_room_service.replace_session_rooms(
        session_id,
        rooms,
        false,
      );
      await Promise.all([
        this.realtime_delivery_service.touch_pending_ttl(session_id),
        this.realtime_room_service.touch_session_rooms(session_id),
      ]);
      this.realtime_connection_service.send_json(client, {
        data: {
          public_key: payload.public_key,
          restored_session: session_result.restored,
          rooms,
          session_id,
        },
        type: 'system.connected',
      });
      await this.realtime_delivery_service.replay_pending_for_session(session_id);
    } catch (error) {
      this.handle_auth_error(client, error);
    }
  }

  @SubscribeMessage('delivery.ack')
  async handle_delivery_ack(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() body: unknown,
  ) {
    try {
      const state = this.get_authenticated_state(client);
      const payload = this.parse_delivery_ack_payload(body);

      await this.realtime_delivery_service.acknowledge(state.session_id, payload);
    } catch (error) {
      this.send_system_error(
        client,
        error instanceof Error ? error.message : 'Invalid delivery ack payload.',
      );
    }
  }

  @SubscribeMessage('chat.message.send')
  async handle_chat_message_send(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() body: unknown,
  ) {
    try {
      const state = this.get_authenticated_state(client);
      await this.direct_message_service.handle_chat_message_send(
        state.public_key,
        body,
      );
    } catch (error) {
      this.send_system_error(
        client,
        error instanceof Error
          ? error.message
          : 'Failed to send the direct message.',
      );
    }
  }

  @SubscribeMessage('chat.message.persisted')
  async handle_chat_message_persisted(
    @ConnectedSocket() client: WebSocket,
    @MessageBody() body: unknown,
  ) {
    try {
      const state = this.get_authenticated_state(client);
      await this.direct_message_service.handle_chat_message_persisted(
        state.public_key,
        body,
      );
    } catch (error) {
      this.send_system_error(
        client,
        error instanceof Error
          ? error.message
          : 'Failed to confirm local message persistence.',
      );
    }
  }

  @SubscribeMessage('chat.sync')
  async handle_chat_sync(@ConnectedSocket() client: WebSocket) {
    try {
      const state = this.get_authenticated_state(client);
      await this.direct_message_service.handle_chat_sync(state.public_key);
    } catch (error) {
      this.send_system_error(
        client,
        error instanceof Error ? error.message : 'Failed to sync chat queue.',
      );
    }
  }

  private async handle_pong(client: WebSocket) {
    this.realtime_connection_service.register_pong(client);

    const state = this.realtime_connection_service.get_authenticated_state(client);

    if (!state?.session_id) {
      return;
    }

    await Promise.all([
      this.realtime_delivery_service.touch_pending_ttl(state.session_id),
      this.realtime_room_service.touch_session_rooms(state.session_id),
      this.realtime_session_service.touch_session(state.session_id),
    ]);
  }

  private handle_auth_error(client: WebSocket, error: unknown) {
    const message =
      error instanceof Error ? error.message : 'Authentication failed.';

    this.send_system_error(client, message);

    if (
      error instanceof UnauthorizedException ||
      error instanceof BadRequestException
    ) {
      client.close(4401, message);
      return;
    }

    client.close(1011, 'Internal realtime error');
  }

  private parse_delivery_ack_payload(body: unknown): DeliveryAckPayload {
    if (typeof body !== 'object' || body === null) {
      throw new BadRequestException('delivery.ack payload must be an object.');
    }

    const payload = body as Record<string, unknown>;

    if (typeof payload.event_id !== 'string' || !payload.event_id.trim()) {
      throw new BadRequestException('event_id is required.');
    }

    if (payload.status !== 'received') {
      throw new BadRequestException('status must be "received".');
    }

    if (typeof payload.received_at !== 'string' || !payload.received_at.trim()) {
      throw new BadRequestException('received_at is required.');
    }

    return {
      event_id: payload.event_id.trim(),
      received_at: payload.received_at.trim(),
      status: 'received',
    };
  }

  private get_authenticated_state(client: WebSocket): AuthenticatedSocketState {
    const state =
      this.realtime_connection_service.get_authenticated_state(client);

    if (!state?.session_id || !state.public_key) {
      this.send_system_error(client, 'Socket is not authenticated.');
      client.close(4401, 'Socket is not authenticated');
      throw new UnauthorizedException('Socket is not authenticated.');
    }

    return state as AuthenticatedSocketState;
  }

  private send_system_error(client: WebSocket, message: string) {
    this.realtime_connection_service.send_json(client, {
      data: {
        message,
      },
      type: 'system.error',
    });
  }
}
