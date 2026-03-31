import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { get_redis_config } from '../../../config/redis.config';
import {
  REALTIME_ROOM_CONTROL_CHANNEL,
  REALTIME_ROOM_FANOUT_PATTERN,
} from '../constants/realtime.constants';
import {
  RoomControlMessage,
  RoomFanoutMessage,
} from '../types/realtime.types';
import { create_room_fanout_channel } from '../utils/realtime_keys.util';

type AsyncHandler<T> = (payload: T) => Promise<void> | void;

@Injectable()
export class RealtimeRedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RealtimeRedisService.name);
  private readonly redis_config = get_redis_config();
  private readonly room_control_handlers = new Set<AsyncHandler<RoomControlMessage>>();
  private readonly room_fanout_handlers = new Set<AsyncHandler<RoomFanoutMessage>>();
  private readonly command_client = this.redis_config.url
    ? new Redis(this.redis_config.url, {
        lazyConnect: true,
        maxRetriesPerRequest: null,
      })
    : null;
  private readonly pub_client = this.command_client?.duplicate({
    lazyConnect: true,
    maxRetriesPerRequest: null,
  });
  private readonly sub_client = this.command_client?.duplicate({
    lazyConnect: true,
    maxRetriesPerRequest: null,
  });

  async onModuleInit() {
    if (!this.command_client || !this.pub_client || !this.sub_client) {
      if (this.redis_config.is_required) {
        throw new Error('REDIS_URL environment variable is required.');
      }

      this.logger.warn('Redis is not configured. Realtime Redis features are off.');
      return;
    }

    this.sub_client.on('message', (channel, message) => {
      void this.handle_channel_message(channel, message);
    });

    this.sub_client.on('pmessage', (pattern, channel, message) => {
      void this.handle_pattern_message(pattern, channel, message);
    });

    await Promise.all([
      this.command_client.connect(),
      this.pub_client.connect(),
      this.sub_client.connect(),
    ]);
    await this.sub_client.subscribe(REALTIME_ROOM_CONTROL_CHANNEL);
    await this.sub_client.psubscribe(REALTIME_ROOM_FANOUT_PATTERN);
  }

  async onModuleDestroy() {
    await Promise.all([
      this.command_client?.quit().catch(() => this.command_client?.disconnect()),
      this.pub_client?.quit().catch(() => this.pub_client?.disconnect()),
      this.sub_client?.quit().catch(() => this.sub_client?.disconnect()),
    ]);
  }

  get_commands_client() {
    if (!this.command_client) {
      throw new Error('Redis command client is not configured.');
    }

    return this.command_client;
  }

  async publish_room_control(message: RoomControlMessage) {
    if (!this.pub_client) {
      throw new Error('Redis publisher is not configured.');
    }

    await this.pub_client.publish(
      REALTIME_ROOM_CONTROL_CHANNEL,
      JSON.stringify(message),
    );
  }

  async publish_room_fanout(message: RoomFanoutMessage) {
    if (!this.pub_client) {
      throw new Error('Redis publisher is not configured.');
    }

    await this.pub_client.publish(
      create_room_fanout_channel(message.room_name),
      JSON.stringify(message),
    );
  }

  register_room_control_handler(handler: AsyncHandler<RoomControlMessage>) {
    this.room_control_handlers.add(handler);

    return () => {
      this.room_control_handlers.delete(handler);
    };
  }

  register_room_fanout_handler(handler: AsyncHandler<RoomFanoutMessage>) {
    this.room_fanout_handlers.add(handler);

    return () => {
      this.room_fanout_handlers.delete(handler);
    };
  }

  private async handle_channel_message(channel: string, message: string) {
    if (channel !== REALTIME_ROOM_CONTROL_CHANNEL) {
      return;
    }

    const payload = JSON.parse(message) as RoomControlMessage;

    await Promise.all(
      [...this.room_control_handlers].map(async (handler) => handler(payload)),
    );
  }

  private async handle_pattern_message(
    pattern: string,
    _channel: string,
    message: string,
  ) {
    if (pattern !== REALTIME_ROOM_FANOUT_PATTERN) {
      return;
    }

    const payload = JSON.parse(message) as RoomFanoutMessage;

    await Promise.all(
      [...this.room_fanout_handlers].map(async (handler) => handler(payload)),
    );
  }
}
