// src/modules/prisma/prisma.service.ts

import 'dotenv/config';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../../generated/prisma/client';

@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, OnModuleDestroy
{
  constructor() {
    const connectionString = process.env.DATABASE_URL;

    if (!connectionString) {
      throw new Error(
        'DATABASE_URL is not set. Add it to open-chat-be/.env or your shell environment before starting the app.',
      );
    }

    super({
      adapter: new PrismaPg({ connectionString }),
    });
  }

  async onModuleInit(): Promise<void> {
    await this.$connect();
    console.log('✅ Prisma connected');
  }

  async onModuleDestroy(): Promise<void> {
    await this.$disconnect();
    console.log('❌ Prisma disconnected');
  }
}
