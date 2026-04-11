import {
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
  UnauthorizedException,
} from '@nestjs/common';
import { DirectMessageModel } from '../../generated/prisma/models/DirectMessage';
import { get_direct_message_config } from '../../config/direct_message.config';
import { PrismaService } from '../prisma/prisma.service';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PeerNetworkService } from '../peer_network/peer_network.service';
import { RealtimeService } from '../realtime/services/realtime.service';
import {
  ChatMessageAcceptedPayload,
  ChatMessageEventPayload,
} from './types/direct_message.types';
import {
  parse_chat_message_persisted_payload,
  parse_chat_message_send_payload,
  verify_direct_message_signature,
} from './utils/direct_message_signature.util';

@Injectable()
export class DirectMessageService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DirectMessageService.name);
  private readonly direct_message_config = get_direct_message_config();
  private cleanup_timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly network_trace_service: NetworkTraceService,
    private readonly peer_network_service: PeerNetworkService,
    private readonly prisma_service: PrismaService,
    private readonly realtime_service: RealtimeService,
  ) {}

  onModuleInit() {
    void this.delete_expired_messages();
    this.cleanup_timer = setInterval(() => {
      void this.delete_expired_messages();
    }, this.direct_message_config.cleanup_interval_ms);
  }

  onModuleDestroy() {
    if (this.cleanup_timer) {
      clearInterval(this.cleanup_timer);
    }
  }

  async handle_chat_message_send(sender_public_key: string, value: unknown) {
    const payload = parse_chat_message_send_payload(value);

    this.trace_event(
      'direct_message.chat_message_send_received',
      'info',
      {
        recipient_public_key: payload.recipient_public_key,
      },
      payload.id,
    );

    const is_valid_signature = await verify_direct_message_signature(
      sender_public_key,
      payload,
    );

    if (!is_valid_signature) {
      this.trace_event(
        'direct_message.signature_rejected',
        'warn',
        {
          reason: 'Direct-message signature verification failed.',
          sender_public_key,
        },
        payload.id,
      );
      throw new UnauthorizedException(
        'Direct-message signature verification failed.',
      );
    }
    this.trace_event(
      'direct_message.signature_verified',
      'info',
      {
        sender_public_key,
      },
      payload.id,
    );

    const [sender, recipient] = await Promise.all([
      this.prisma_service.user.findUnique({
        where: {
          public_key: sender_public_key,
        },
        select: {
          public_key: true,
          x25519_public_key: true,
        },
      }),
      this.prisma_service.user.findUnique({
        where: {
          public_key: payload.recipient_public_key,
        },
        select: {
          public_key: true,
          x25519_public_key: true,
        },
      }),
    ]);

    if (!sender?.x25519_public_key) {
      this.trace_event(
        'direct_message.sender_x25519_missing',
        'warn',
        undefined,
        payload.id,
      );
      throw new NotFoundException(
        'Sender X25519 public key is not registered on the server.',
      );
    }

    if (!recipient?.x25519_public_key) {
      this.trace_event(
        'direct_message.recipient_x25519_missing',
        'warn',
        {
          recipient_public_key: payload.recipient_public_key,
        },
        payload.id,
      );
      throw new NotFoundException(
        'Recipient X25519 public key is not available.',
      );
    }

    const queued_message = await this.prisma_service.directMessage.upsert({
      where: {
        id: payload.id,
      },
      update: {},
      create: {
        algorithm: payload.algorithm,
        expiresAt: this.create_expiry_date(),
        id: payload.id,
        message: payload.message,
        message_hash: payload.message_hash,
        recipient_public_key: payload.recipient_public_key,
        send_time: new Date(payload.send_time),
        sender_public_key: sender_public_key,
        sender_signature: payload.sender_signature,
        sender_x25519_public_key: sender.x25519_public_key,
      },
    });
    const message_event = this.map_chat_message_event(queued_message);
    const accepted_event = this.map_chat_message_accepted_event(queued_message);

    this.trace_event(
      'direct_message.queue_upserted',
      'info',
      {
        recipient_public_key: payload.recipient_public_key,
      },
      queued_message.id,
    );

    await this.realtime_service.emit_to_user(payload.recipient_public_key, {
      payload: message_event,
      type: 'chat.message',
    });
    this.trace_event(
      'direct_message.recipient_emit_sent',
      'info',
      {
        recipient_public_key: payload.recipient_public_key,
      },
      queued_message.id,
    );
    await this.realtime_service.emit_to_user(sender_public_key, {
      payload: accepted_event,
      type: 'chat.message.accepted',
    });
    this.trace_event(
      'direct_message.sender_accepted_emit_sent',
      'info',
      {
        sender_public_key,
      },
      queued_message.id,
    );

    return accepted_event;
  }

  async handle_chat_message_persisted(
    recipient_public_key: string,
    value: unknown,
  ) {
    const payload = parse_chat_message_persisted_payload(value);
    this.trace_event(
      'direct_message.chat_message_persisted_received',
      'info',
      {
        recipient_public_key,
      },
      payload.message_id,
    );
    const queued_message = await this.prisma_service.directMessage.findUnique({
      where: {
        id: payload.message_id,
      },
      select: {
        id: true,
        recipient_public_key: true,
      },
    });

    if (!queued_message) {
      this.trace_event(
        'direct_message.queue_row_not_found',
        'info',
        undefined,
        payload.message_id,
      );
      return {
        deleted: false,
        message_id: payload.message_id,
      };
    }

    if (queued_message.recipient_public_key !== recipient_public_key) {
      this.trace_event(
        'direct_message.queue_delete_unauthorized',
        'warn',
        {
          queued_recipient_public_key: queued_message.recipient_public_key,
          recipient_public_key,
        },
        payload.message_id,
      );
      throw new UnauthorizedException(
        'This message does not belong to the authenticated recipient.',
      );
    }

    await this.prisma_service.directMessage.delete({
      where: {
        id: payload.message_id,
      },
    });
    await this.peer_network_service.publish_dm_delete_gossip(
      payload.message_id,
      recipient_public_key,
    );
    this.trace_event(
      'direct_message.queue_row_deleted',
      'info',
      {
        recipient_public_key,
      },
      payload.message_id,
    );

    return {
      deleted: true,
      message_id: payload.message_id,
    };
  }

  async handle_chat_sync(recipient_public_key: string) {
    this.trace_event('direct_message.chat_sync_requested', 'info', {
      recipient_public_key,
    });

    const queued_messages = await this.prisma_service.directMessage.findMany({
      where: {
        expiresAt: {
          gt: new Date(),
        },
        recipient_public_key,
      },
      orderBy: {
        send_time: 'asc',
      },
    });

    for (const queued_message of queued_messages) {
      await this.realtime_service.emit_to_user(recipient_public_key, {
        payload: this.map_chat_message_event(queued_message),
        type: 'chat.message',
      });
      this.trace_event(
        'direct_message.sync_replay_emitted',
        'info',
        {
          recipient_public_key,
        },
        queued_message.id,
      );
    }

    return {
      count: queued_messages.length,
    };
  }

  private async delete_expired_messages() {
    const deleted_messages = await this.prisma_service.directMessage.deleteMany(
      {
        where: {
          expiresAt: {
            lte: new Date(),
          },
        },
      },
    );

    if (deleted_messages.count > 0) {
      this.logger.log(
        `Deleted ${deleted_messages.count} expired direct-message queue rows.`,
      );
      this.trace_event('direct_message.expired_rows_deleted', 'info', {
        count: deleted_messages.count,
      });
    }
  }

  private create_expiry_date() {
    const expires_at = new Date();

    expires_at.setDate(
      expires_at.getDate() + this.direct_message_config.retention_days,
    );

    return expires_at;
  }

  private map_chat_message_event(
    direct_message: DirectMessageModel,
  ): ChatMessageEventPayload {
    return {
      algorithm: direct_message.algorithm,
      expires_at: direct_message.expiresAt.toISOString(),
      id: direct_message.id,
      message: direct_message.message,
      message_hash: direct_message.message_hash,
      send_time: direct_message.send_time.toISOString(),
      sender_public_key: direct_message.sender_public_key,
      sender_signature: direct_message.sender_signature,
      sender_x25519_public_key: direct_message.sender_x25519_public_key,
    };
  }

  private map_chat_message_accepted_event(
    direct_message: DirectMessageModel,
  ): ChatMessageAcceptedPayload {
    return {
      expires_at: direct_message.expiresAt.toISOString(),
      id: direct_message.id,
      recipient_public_key: direct_message.recipient_public_key,
      send_time: direct_message.send_time.toISOString(),
    };
  }

  private trace_event(
    event_type: string,
    severity: 'error' | 'info' | 'warn',
    details?: unknown,
    message_id?: string,
  ) {
    this.network_trace_service.record_event({
      details,
      event_type,
      message_id,
      severity,
      source: 'direct_message',
    });
  }
}
