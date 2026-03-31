import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { REALTIME_NODE_ID } from '../constants/realtime.constants';
import {
  ReliableEnvelope,
  ReliableEnvelopeInput,
  RoomFanoutMessage,
} from '../types/realtime.types';
import { RealtimeDeliveryService } from './realtime_delivery.service';
import { RealtimeRedisService } from './realtime_redis.service';
import { RealtimeRoomService } from './realtime_room.service';

@Injectable()
export class RealtimeFanoutService implements OnModuleInit, OnModuleDestroy {
  private unsubscribe_room_fanout: (() => void) | null = null;

  constructor(
    @Inject(REALTIME_NODE_ID) private readonly node_id: string,
    private readonly realtime_delivery_service: RealtimeDeliveryService,
    private readonly realtime_redis_service: RealtimeRedisService,
    private readonly realtime_room_service: RealtimeRoomService,
  ) {}

  onModuleInit() {
    this.unsubscribe_room_fanout =
      this.realtime_redis_service.register_room_fanout_handler(async (message) => {
        await this.handle_room_fanout(message);
      });
  }

  onModuleDestroy() {
    this.unsubscribe_room_fanout?.();
  }

  async emit_to_room(room_name: string, input: ReliableEnvelopeInput) {
    const envelope = this.realtime_delivery_service.create_envelope(input);

    await this.deliver_to_local_room(room_name, envelope);
    await this.realtime_redis_service.publish_room_fanout({
      envelope,
      origin_node_id: this.node_id,
      room_name,
    });

    return envelope;
  }

  private async handle_room_fanout(message: RoomFanoutMessage) {
    if (message.origin_node_id === this.node_id) {
      return;
    }

    await this.deliver_to_local_room(message.room_name, message.envelope);
  }

  private async deliver_to_local_room(
    room_name: string,
    envelope: ReliableEnvelope,
  ) {
    const session_ids = this.realtime_room_service.get_local_session_ids(room_name);

    await Promise.all(
      session_ids.map(async (session_id) => {
        await this.realtime_delivery_service.deliver_to_session(session_id, envelope);
      }),
    );
  }
}
