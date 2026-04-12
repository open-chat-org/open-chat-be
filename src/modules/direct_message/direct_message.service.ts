import {
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { DirectMessageModel } from '../../generated/prisma/models/DirectMessage';
import { get_direct_message_config } from '../../config/direct_message.config';
import { get_inter_server_dm_config } from '../../config/inter_server_dm.config';
import { PrismaService } from '../prisma/prisma.service';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PeerNetworkService } from '../peer_network/peer_network.service';
import { RealtimeService } from '../realtime/services/realtime.service';
import {
  InterServerDmDeleteAckPayload,
  InterServerDmDeleteRequestPayload,
  InterServerDmPullRequestPayload,
  InterServerDmPullResponsePayload,
  InterServerDmReplicateAckPayload,
  InterServerDmReplicatePayload,
} from '../peer_network/types/inter_server_dm.types';
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
  private readonly inter_server_dm_config = get_inter_server_dm_config();
  private cleanup_timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly network_trace_service: NetworkTraceService,
    private readonly peer_network_service: PeerNetworkService,
    private readonly prisma_service: PrismaService,
    private readonly realtime_service: RealtimeService,
  ) {}

  onModuleInit() {
    this.peer_network_service.register_inter_server_dm_callbacks({
      on_delete_request: async (payload, source_peer_id) =>
        this.handle_inter_server_delete_request(payload, source_peer_id),
      on_pull_request: async (payload, source_peer_id) =>
        this.handle_inter_server_pull_request(payload, source_peer_id),
      on_replicate_request: async (payload, source_peer_id) =>
        this.handle_inter_server_replicate_request(payload, source_peer_id),
    });
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

    const replica_peer_ids =
      await this.peer_network_service.select_replica_peer_ids_for_user(
        payload.recipient_public_key,
        this.inter_server_dm_config.replica_remote_count,
      );

    if (
      replica_peer_ids.length < this.inter_server_dm_config.replica_remote_count
    ) {
      this.trace_event(
        'direct_message.replica_selection_insufficient',
        'warn',
        {
          replica_peer_count: replica_peer_ids.length,
          required_remote_count: this.inter_server_dm_config.replica_remote_count,
        },
        payload.id,
      );
      throw new ServiceUnavailableException(
        'Unable to select enough remote replica servers for this recipient.',
      );
    }

    const local_peer_id = this.peer_network_service.get_local_peer_id_value();
    let effective_replica_peer_ids = Array.from(new Set(replica_peer_ids));

    const queued_message = await this.prisma_service.directMessage.upsert({
      where: {
        id: payload.id,
      },
      update: {
        algorithm: payload.algorithm,
        expiresAt: this.create_expiry_date(),
        is_replica_copy: false,
        message: payload.message,
        message_hash: payload.message_hash,
        origin_server_peer_id: local_peer_id,
        recipient_public_key: payload.recipient_public_key,
        replica_peer_ids: effective_replica_peer_ids,
        send_time: new Date(payload.send_time),
        sender_public_key: sender_public_key,
        sender_signature: payload.sender_signature,
        sender_x25519_public_key: sender.x25519_public_key,
      },
      create: {
        algorithm: payload.algorithm,
        expiresAt: this.create_expiry_date(),
        id: payload.id,
        is_replica_copy: false,
        message: payload.message,
        message_hash: payload.message_hash,
        origin_server_peer_id: local_peer_id,
        recipient_public_key: payload.recipient_public_key,
        replica_peer_ids: effective_replica_peer_ids,
        send_time: new Date(payload.send_time),
        sender_public_key: sender_public_key,
        sender_signature: payload.sender_signature,
        sender_x25519_public_key: sender.x25519_public_key,
      },
    });
    const replicate_payload: InterServerDmReplicatePayload = {
      algorithm: queued_message.algorithm,
      id: queued_message.id,
      message: queued_message.message,
      message_hash: queued_message.message_hash,
      origin_server_peer_id: local_peer_id,
      recipient_public_key: queued_message.recipient_public_key,
      replica_peer_ids: effective_replica_peer_ids,
      send_time: queued_message.send_time.toISOString(),
      sender_public_key: queued_message.sender_public_key,
      sender_signature: queued_message.sender_signature,
      sender_x25519_public_key: queued_message.sender_x25519_public_key,
    };
    const replicate_result =
      await this.peer_network_service.replicate_direct_message_to_peers({
        payload: replicate_payload,
        peer_ids: effective_replica_peer_ids,
        require_quorum: true,
      });

    if (!replicate_result.quorum_met) {
      await this.prisma_service.directMessage.deleteMany({
        where: {
          id: payload.id,
        },
      });
      this.trace_event(
        'direct_message.replication_quorum_failed',
        'warn',
        {
          acknowledged_peer_count: replicate_result.acknowledged_peer_ids.length,
          failed_peer_count: replicate_result.failed_peer_ids.length,
          required_quorum: this.inter_server_dm_config.replica_remote_quorum,
        },
        payload.id,
      );
      throw new ServiceUnavailableException(
        'Failed to replicate direct message to required remote quorum.',
      );
    }

    const presence = await this.peer_network_service.query_user_presence(
      payload.recipient_public_key,
      effective_replica_peer_ids,
    );
    const live_delivery_peer_ids = presence.online_server_peer_ids.filter(
      (peer_id) =>
        peer_id &&
        peer_id !== local_peer_id &&
        !effective_replica_peer_ids.includes(peer_id),
    );

    if (live_delivery_peer_ids.length > 0) {
      effective_replica_peer_ids = Array.from(
        new Set([...effective_replica_peer_ids, ...live_delivery_peer_ids]),
      );
      await this.prisma_service.directMessage.update({
        where: {
          id: queued_message.id,
        },
        data: {
          replica_peer_ids: effective_replica_peer_ids,
        },
      });
      await this.peer_network_service.replicate_direct_message_to_peers({
        payload: {
          ...replicate_payload,
          replica_peer_ids: effective_replica_peer_ids,
        },
        peer_ids: live_delivery_peer_ids,
        require_quorum: false,
      });
    }

    const has_local_recipient_session =
      await this.realtime_service.has_active_session(
        payload.recipient_public_key,
      );
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

    if (has_local_recipient_session) {
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
    }

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
        origin_server_peer_id: true,
        replica_peer_ids: true,
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
        deleted: true,
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

    const origin_server_peer_id =
      queued_message.origin_server_peer_id?.trim() ||
      this.peer_network_service.get_local_peer_id_value();
    const replica_peer_ids = this.normalize_peer_id_list(
      queued_message.replica_peer_ids,
    );

    await this.peer_network_service.send_targeted_dm_delete({
      message_id: payload.message_id,
      origin_server_peer_id,
      recipient_public_key,
      replica_peer_ids,
    });

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

    await this.synchronize_user_queue_from_replicas(recipient_public_key);

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

  async handle_user_connected(public_key: string) {
    await this.synchronize_user_queue_from_replicas(public_key);
  }

  private async handle_inter_server_replicate_request(
    payload: InterServerDmReplicatePayload,
    source_peer_id: string,
  ): Promise<InterServerDmReplicateAckPayload> {
    const is_valid_signature = await verify_direct_message_signature(
      payload.sender_public_key,
      {
        algorithm: payload.algorithm,
        id: payload.id,
        message: payload.message,
        message_hash: payload.message_hash,
        recipient_public_key: payload.recipient_public_key,
        sender_signature: payload.sender_signature,
        send_time: payload.send_time,
      },
    );

    if (!is_valid_signature) {
      this.trace_event(
        'direct_message.inter_server_replicate_signature_rejected',
        'warn',
        {
          source_peer_id,
        },
        payload.id,
      );
      return {
        id: payload.id,
        stored: false,
      };
    }

    const local_peer_id = this.peer_network_service.get_local_peer_id_value();
    const origin_server_peer_id =
      payload.origin_server_peer_id?.trim() || source_peer_id;
    const replica_peer_ids = this.normalize_peer_id_list(
      payload.replica_peer_ids,
    );
    const expires_at = this.create_expiry_date_from_send_time(payload.send_time);

    const stored_message = await this.prisma_service.directMessage.upsert({
      where: {
        id: payload.id,
      },
      update: {
        algorithm: payload.algorithm,
        expiresAt: expires_at,
        is_replica_copy: origin_server_peer_id !== local_peer_id,
        message: payload.message,
        message_hash: payload.message_hash,
        origin_server_peer_id,
        recipient_public_key: payload.recipient_public_key,
        replica_peer_ids,
        send_time: new Date(payload.send_time),
        sender_public_key: payload.sender_public_key,
        sender_signature: payload.sender_signature,
        sender_x25519_public_key: payload.sender_x25519_public_key,
      },
      create: {
        algorithm: payload.algorithm,
        expiresAt: expires_at,
        id: payload.id,
        is_replica_copy: origin_server_peer_id !== local_peer_id,
        message: payload.message,
        message_hash: payload.message_hash,
        origin_server_peer_id,
        recipient_public_key: payload.recipient_public_key,
        replica_peer_ids,
        send_time: new Date(payload.send_time),
        sender_public_key: payload.sender_public_key,
        sender_signature: payload.sender_signature,
        sender_x25519_public_key: payload.sender_x25519_public_key,
      },
    });

    const has_active_session = await this.realtime_service.has_active_session(
      payload.recipient_public_key,
    );

    if (has_active_session) {
      await this.realtime_service.emit_to_user(payload.recipient_public_key, {
        payload: this.map_chat_message_event(stored_message),
        type: 'chat.message',
      });
      this.trace_event(
        'direct_message.inter_server_live_delivery_sent',
        'info',
        {
          recipient_public_key: payload.recipient_public_key,
          source_peer_id,
        },
        payload.id,
      );
    }

    return {
      id: payload.id,
      stored: true,
    };
  }

  private async handle_inter_server_pull_request(
    payload: InterServerDmPullRequestPayload,
    source_peer_id: string,
  ): Promise<InterServerDmPullResponsePayload> {
    const limit = Math.max(
      1,
      Math.min(
        this.inter_server_dm_config.pull_batch_size,
        Math.trunc(payload.limit || this.inter_server_dm_config.pull_batch_size),
      ),
    );
    const rows = await this.prisma_service.directMessage.findMany({
      where: {
        ...(payload.cursor
          ? {
              id: {
                gt: payload.cursor,
              },
            }
          : {}),
        expiresAt: {
          gt: new Date(),
        },
        recipient_public_key: payload.public_key,
      },
      orderBy: {
        id: 'asc',
      },
      take: limit,
    });
    const next_cursor =
      rows.length === limit ? rows[rows.length - 1].id : null;

    this.trace_event('direct_message.inter_server_pull_served', 'info', {
      count: rows.length,
      public_key: payload.public_key,
      source_peer_id,
    });

    return {
      items: rows.map((row) => this.map_to_inter_server_replicate_payload(row)),
      next_cursor,
      public_key: payload.public_key,
    };
  }

  private async handle_inter_server_delete_request(
    payload: InterServerDmDeleteRequestPayload,
    source_peer_id: string,
  ): Promise<InterServerDmDeleteAckPayload> {
    const existing_message = await this.prisma_service.directMessage.findUnique({
      where: {
        id: payload.message_id,
      },
      select: {
        origin_server_peer_id: true,
        recipient_public_key: true,
        replica_peer_ids: true,
      },
    });

    if (!existing_message) {
      return {
        deleted: true,
        message_id: payload.message_id,
      };
    }

    const authorized_peer_ids = new Set<string>([
      existing_message.origin_server_peer_id?.trim() || '',
      ...this.normalize_peer_id_list(existing_message.replica_peer_ids),
    ].filter((peer_id) => Boolean(peer_id)));

    if (
      authorized_peer_ids.size > 0 &&
      !authorized_peer_ids.has(source_peer_id)
    ) {
      this.trace_event(
        'direct_message.inter_server_delete_rejected',
        'warn',
        {
          reason: 'Source peer is not authorized to delete this message.',
          source_peer_id,
        },
        payload.message_id,
      );
      return {
        deleted: false,
        message_id: payload.message_id,
      };
    }

    if (existing_message.recipient_public_key !== payload.recipient_public_key) {
      this.trace_event(
        'direct_message.inter_server_delete_rejected',
        'warn',
        {
          reason: 'Recipient mismatch during inter-server delete.',
          source_peer_id,
        },
        payload.message_id,
      );
      return {
        deleted: false,
        message_id: payload.message_id,
      };
    }

    await this.prisma_service.directMessage.delete({
      where: {
        id: payload.message_id,
      },
    });

    this.trace_event(
      'direct_message.inter_server_delete_applied',
      'info',
      {
        source_peer_id,
      },
      payload.message_id,
    );

    return {
      deleted: true,
      message_id: payload.message_id,
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

  private create_expiry_date_from_send_time(send_time: string) {
    const base_time = new Date(send_time);

    if (Number.isNaN(base_time.getTime())) {
      return this.create_expiry_date();
    }

    base_time.setDate(
      base_time.getDate() + this.direct_message_config.retention_days,
    );

    return base_time;
  }

  private map_to_inter_server_replicate_payload(
    direct_message: DirectMessageModel,
  ): InterServerDmReplicatePayload {
    return {
      algorithm: direct_message.algorithm,
      id: direct_message.id,
      message: direct_message.message,
      message_hash: direct_message.message_hash,
      origin_server_peer_id:
        direct_message.origin_server_peer_id?.trim() ||
        this.peer_network_service.get_local_peer_id_value(),
      recipient_public_key: direct_message.recipient_public_key,
      replica_peer_ids: this.normalize_peer_id_list(
        direct_message.replica_peer_ids,
      ),
      send_time: direct_message.send_time.toISOString(),
      sender_public_key: direct_message.sender_public_key,
      sender_signature: direct_message.sender_signature,
      sender_x25519_public_key: direct_message.sender_x25519_public_key,
    };
  }

  private normalize_peer_id_list(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return Array.from(
      new Set(
        value
          .filter((item): item is string => typeof item === 'string')
          .map((item) => item.trim())
          .filter((item) => item.length > 0 && item !== 'unknown_peer'),
      ),
    );
  }

  private async synchronize_user_queue_from_replicas(public_key: string) {
    const replica_peer_ids =
      await this.peer_network_service.select_replica_peer_ids_for_user(
        public_key,
        this.inter_server_dm_config.replica_remote_count,
      );

    if (replica_peer_ids.length === 0) {
      return;
    }

    await this.peer_network_service.announce_user_presence(
      public_key,
      replica_peer_ids,
    );

    const pending_messages =
      await this.peer_network_service.pull_pending_messages_from_replicas(
        public_key,
        replica_peer_ids,
      );

    if (pending_messages.length === 0) {
      return;
    }

    let synced_count = 0;

    for (const pending_message of pending_messages) {
      const is_valid_signature = await verify_direct_message_signature(
        pending_message.sender_public_key,
        {
          algorithm: pending_message.algorithm,
          id: pending_message.id,
          message: pending_message.message,
          message_hash: pending_message.message_hash,
          recipient_public_key: pending_message.recipient_public_key,
          sender_signature: pending_message.sender_signature,
          send_time: pending_message.send_time,
        },
      );

      if (!is_valid_signature) {
        this.trace_event(
          'direct_message.pull_signature_rejected',
          'warn',
          {
            sender_public_key: pending_message.sender_public_key,
          },
          pending_message.id,
        );
        continue;
      }

      const expires_at = this.create_expiry_date_from_send_time(
        pending_message.send_time,
      );

      await this.prisma_service.directMessage.upsert({
        where: {
          id: pending_message.id,
        },
        update: {
          algorithm: pending_message.algorithm,
          expiresAt: expires_at,
          is_replica_copy: true,
          message: pending_message.message,
          message_hash: pending_message.message_hash,
          origin_server_peer_id:
            pending_message.origin_server_peer_id?.trim() || null,
          recipient_public_key: pending_message.recipient_public_key,
          replica_peer_ids: this.normalize_peer_id_list(
            pending_message.replica_peer_ids,
          ),
          send_time: new Date(pending_message.send_time),
          sender_public_key: pending_message.sender_public_key,
          sender_signature: pending_message.sender_signature,
          sender_x25519_public_key: pending_message.sender_x25519_public_key,
        },
        create: {
          algorithm: pending_message.algorithm,
          expiresAt: expires_at,
          id: pending_message.id,
          is_replica_copy: true,
          message: pending_message.message,
          message_hash: pending_message.message_hash,
          origin_server_peer_id:
            pending_message.origin_server_peer_id?.trim() || null,
          recipient_public_key: pending_message.recipient_public_key,
          replica_peer_ids: this.normalize_peer_id_list(
            pending_message.replica_peer_ids,
          ),
          send_time: new Date(pending_message.send_time),
          sender_public_key: pending_message.sender_public_key,
          sender_signature: pending_message.sender_signature,
          sender_x25519_public_key: pending_message.sender_x25519_public_key,
        },
      });
      synced_count += 1;
    }

    this.trace_event('direct_message.pull_sync_applied', 'info', {
      public_key,
      synced_count,
    });
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
