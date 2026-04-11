import { Injectable } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { PrismaService } from '../../../prisma/prisma.service';
import {
  SYNC_TABLE_ORDER,
  SyncFetchByKeysRequest,
  SyncFetchByKeysResponse,
  SyncFetchRequest,
  SyncFetchResponse,
  SyncManifestResponse,
  SyncRowEnvelope,
  SyncTableName,
  SyncVerifyBatchRequest,
  SyncVerifyBatchResponse,
} from '../types/sync_wire.types';

@Injectable()
export class TableSyncRunnerService {
  constructor(private readonly prisma_service: PrismaService) {}

  async create_manifest_response(run_id: string, local_peer_id: string) {
    const [
      server_node_count,
      server_score_report_count,
      server_score_aggregate_count,
      user_count,
      direct_message_count,
    ] = await Promise.all([
      this.prisma_service.serverNode.count(),
      this.prisma_service.serverScoreReport.count(),
      this.prisma_service.serverScoreAggregate.count(),
      this.prisma_service.user.count(),
      this.prisma_service.directMessage.count({
        where: {
          expiresAt: {
            gt: new Date(),
          },
        },
      }),
    ]);

    return {
      generated_at: new Date().toISOString(),
      local_peer_id,
      run_id,
      tables: [
        {
          row_count: server_node_count,
          table: 'server_node',
        },
        {
          row_count: server_score_report_count,
          table: 'server_score_report',
        },
        {
          row_count: server_score_aggregate_count,
          table: 'server_score_aggregate',
        },
        {
          row_count: user_count,
          table: 'user',
        },
        {
          row_count: direct_message_count,
          table: 'direct_message',
        },
      ],
      type: 'sync.manifest.response',
    } satisfies SyncManifestResponse;
  }

  async handle_fetch_request(input: SyncFetchRequest) {
    const limit = Math.max(1, Math.min(1000, Math.trunc(input.limit)));
    const rows = await this.fetch_table_rows(
      input.table,
      input.cursor ?? null,
      limit,
    );
    const items = rows.map((row) => this.to_row_envelope(input.table, row));
    const last_row = rows[rows.length - 1];
    const next_cursor =
      rows.length === limit && last_row
        ? this.extract_primary_key(input.table, last_row)
        : null;

    return {
      items,
      next_cursor,
      run_id: input.run_id,
      table: input.table,
      type: 'sync.fetch.response',
    } satisfies SyncFetchResponse;
  }

  async handle_verify_request(input: SyncVerifyBatchRequest) {
    const keys = input.rows.map((row) => row.primary_key);
    const local_rows = await this.fetch_rows_by_keys(input.table, keys);
    const local_rows_map = new Map(
      local_rows.map((row): [string, any] => [
        this.extract_primary_key(input.table, row),
        row,
      ]),
    );

    return {
      run_id: input.run_id,
      table: input.table,
      type: 'sync.verify.response',
      votes: input.rows.map((row) => {
        const local_row = local_rows_map.get(row.primary_key);
        const canonical_hash = local_row
          ? this.to_row_envelope(input.table, local_row).canonical_hash
          : null;

        return {
          canonical_hash,
          primary_key: row.primary_key,
        };
      }),
    } satisfies SyncVerifyBatchResponse;
  }

  async handle_fetch_by_keys_request(input: SyncFetchByKeysRequest) {
    const rows = await this.fetch_rows_by_keys(input.table, input.keys);

    return {
      items: rows.map((row) => this.to_row_envelope(input.table, row)),
      run_id: input.run_id,
      table: input.table,
      type: 'sync.fetch_by_keys.response',
    } satisfies SyncFetchByKeysResponse;
  }

  async apply_rows(rows: SyncRowEnvelope[]) {
    for (const row of rows) {
      await this.apply_single_row(row);
    }
  }

  get_table_order() {
    return [...SYNC_TABLE_ORDER];
  }

  private async fetch_table_rows(
    table: SyncTableName,
    cursor: string | null,
    limit: number,
  ) {
    switch (table) {
      case 'server_node':
        return this.prisma_service.serverNode.findMany({
          ...(cursor
            ? {
                where: {
                  peer_id: {
                    gt: cursor,
                  },
                },
              }
            : {}),
          orderBy: {
            peer_id: 'asc',
          },
          take: limit,
        });
      case 'server_score_report':
        return this.prisma_service.serverScoreReport.findMany({
          ...(cursor
            ? {
                where: {
                  id: {
                    gt: cursor,
                  },
                },
              }
            : {}),
          orderBy: {
            id: 'asc',
          },
          take: limit,
        });
      case 'server_score_aggregate':
        return this.prisma_service.serverScoreAggregate.findMany({
          ...(cursor
            ? {
                where: {
                  target_peer_id: {
                    gt: cursor,
                  },
                },
              }
            : {}),
          orderBy: {
            target_peer_id: 'asc',
          },
          take: limit,
        });
      case 'user':
        return this.prisma_service.user.findMany({
          ...(cursor
            ? {
                where: {
                  public_key: {
                    gt: cursor,
                  },
                },
              }
            : {}),
          orderBy: {
            public_key: 'asc',
          },
          take: limit,
        });
      case 'direct_message':
        return this.prisma_service.directMessage.findMany({
          where: {
            ...(cursor
              ? {
                  id: {
                    gt: cursor,
                  },
                }
              : {}),
            expiresAt: {
              gt: new Date(),
            },
          },
          orderBy: {
            id: 'asc',
          },
          take: limit,
        });
      default:
        return [];
    }
  }

  private async fetch_rows_by_keys(table: SyncTableName, keys: string[]) {
    if (keys.length === 0) {
      return [];
    }

    switch (table) {
      case 'server_node':
        return this.prisma_service.serverNode.findMany({
          where: {
            peer_id: {
              in: keys,
            },
          },
        });
      case 'server_score_report':
        return this.prisma_service.serverScoreReport.findMany({
          where: {
            id: {
              in: keys,
            },
          },
        });
      case 'server_score_aggregate':
        return this.prisma_service.serverScoreAggregate.findMany({
          where: {
            target_peer_id: {
              in: keys,
            },
          },
        });
      case 'user':
        return this.prisma_service.user.findMany({
          where: {
            public_key: {
              in: keys,
            },
          },
        });
      case 'direct_message':
        return this.prisma_service.directMessage.findMany({
          where: {
            expiresAt: {
              gt: new Date(),
            },
            id: {
              in: keys,
            },
          },
        });
      default:
        return [];
    }
  }

  private extract_primary_key(table: SyncTableName, row: any) {
    switch (table) {
      case 'server_node':
        return String(row.peer_id);
      case 'server_score_report':
      case 'direct_message':
        return String(row.id);
      case 'server_score_aggregate':
        return String(row.target_peer_id);
      case 'user':
        return String(row.public_key);
      default:
        return '';
    }
  }

  private to_row_envelope(table: SyncTableName, row: any): SyncRowEnvelope {
    const primary_key = this.extract_primary_key(table, row);
    const updated_at = this.extract_updated_at(table, row);
    const payload = this.to_payload(table, row);
    const canonical_hash = this.create_row_hash(
      table,
      primary_key,
      payload,
      updated_at,
    );

    return {
      canonical_hash,
      payload,
      primary_key,
      table,
      updated_at,
    };
  }

  private extract_updated_at(table: SyncTableName, row: any) {
    switch (table) {
      case 'server_score_aggregate':
        return new Date(row.updated_at).toISOString();
      case 'direct_message':
        return new Date(row.createdAt).toISOString();
      default:
        return new Date(row.updatedAt).toISOString();
    }
  }

  private to_payload(table: SyncTableName, row: any): Record<string, unknown> {
    switch (table) {
      case 'server_node':
        return {
          created_at: new Date(row.createdAt).toISOString(),
          is_active: row.is_active,
          last_announce_at: row.last_announce_at
            ? new Date(row.last_announce_at).toISOString()
            : null,
          last_seen_at: new Date(row.last_seen_at).toISOString(),
          listen_addresses: row.listen_addresses,
          peer_id: row.peer_id,
          server_public_key: row.server_public_key,
          updated_at: new Date(row.updatedAt).toISOString(),
        };
      case 'server_score_report':
        return {
          created_at: new Date(row.createdAt).toISOString(),
          expires_at: new Date(row.expires_at).toISOString(),
          id: row.id,
          observed_at: new Date(row.observed_at).toISOString(),
          payload_hash: row.payload_hash,
          reporter_peer_id: row.reporter_peer_id,
          reporter_server_public_key: row.reporter_server_public_key,
          score: row.score,
          signature: row.signature,
          target_peer_id: row.target_peer_id,
          updated_at: new Date(row.updatedAt).toISOString(),
        };
      case 'server_score_aggregate':
        return {
          last_report_at: new Date(row.last_report_at).toISOString(),
          mean_score: row.mean_score,
          report_count: row.report_count,
          target_peer_id: row.target_peer_id,
          updated_at: new Date(row.updated_at).toISOString(),
        };
      case 'user':
        return {
          created_at: new Date(row.createdAt).toISOString(),
          email: row.email,
          name: row.name,
          phone: row.phone,
          public_key: row.public_key,
          quote: row.quote,
          updated_at: new Date(row.updatedAt).toISOString(),
          username: row.username,
          verfiication_id: row.verfiication_id,
          x25519_public_key: row.x25519_public_key,
        };
      case 'direct_message':
        return {
          algorithm: row.algorithm,
          created_at: new Date(row.createdAt).toISOString(),
          expires_at: new Date(row.expiresAt).toISOString(),
          id: row.id,
          message: row.message,
          message_hash: row.message_hash,
          recipient_public_key: row.recipient_public_key,
          send_time: new Date(row.send_time).toISOString(),
          sender_public_key: row.sender_public_key,
          sender_signature: row.sender_signature,
          sender_x25519_public_key: row.sender_x25519_public_key,
        };
      default:
        return {};
    }
  }

  private create_row_hash(
    table: SyncTableName,
    primary_key: string,
    payload: Record<string, unknown>,
    updated_at: string,
  ) {
    const canonical_json = JSON.stringify({
      payload,
      primary_key,
      table,
      updated_at,
    });
    const hash = createHash('sha256').update(canonical_json).digest('hex');

    return `0x${hash}`;
  }

  private async apply_single_row(row: SyncRowEnvelope) {
    switch (row.table) {
      case 'server_node':
        await this.apply_server_node(row);
        return;
      case 'server_score_report':
        await this.apply_server_score_report(row);
        return;
      case 'server_score_aggregate':
        await this.apply_server_score_aggregate(row);
        return;
      case 'user':
        await this.apply_user(row);
        return;
      case 'direct_message':
        await this.apply_direct_message(row);
        return;
      default:
        return;
    }
  }

  private async apply_server_node(row: SyncRowEnvelope) {
    const payload = row.payload;
    const listen_addresses = Array.isArray(payload.listen_addresses)
      ? payload.listen_addresses
      : undefined;
    const existing = await this.prisma_service.serverNode.findUnique({
      where: {
        peer_id: row.primary_key,
      },
      select: {
        updatedAt: true,
      },
    });

    if (
      existing &&
      existing.updatedAt.getTime() > new Date(row.updated_at).getTime()
    ) {
      return;
    }

    await this.prisma_service.serverNode.upsert({
      where: {
        peer_id: row.primary_key,
      },
      update: {
        is_active: Boolean(payload.is_active),
        last_announce_at: payload.last_announce_at
          ? new Date(String(payload.last_announce_at))
          : null,
        last_seen_at: new Date(String(payload.last_seen_at)),
        listen_addresses,
        server_public_key: payload.server_public_key
          ? String(payload.server_public_key)
          : null,
      },
      create: {
        is_active: Boolean(payload.is_active),
        last_announce_at: payload.last_announce_at
          ? new Date(String(payload.last_announce_at))
          : null,
        last_seen_at: new Date(String(payload.last_seen_at)),
        listen_addresses,
        peer_id: row.primary_key,
        server_public_key: payload.server_public_key
          ? String(payload.server_public_key)
          : null,
      },
    });
  }

  private async apply_server_score_report(row: SyncRowEnvelope) {
    const payload = row.payload;
    const observed_at = new Date(String(payload.observed_at));
    const existing = await this.prisma_service.serverScoreReport.findUnique({
      where: {
        id: row.primary_key,
      },
      select: {
        updatedAt: true,
      },
    });

    if (
      existing &&
      existing.updatedAt.getTime() > new Date(row.updated_at).getTime()
    ) {
      return;
    }

    await this.prisma_service.serverScoreReport.upsert({
      where: {
        id: row.primary_key,
      },
      update: {
        expires_at: new Date(String(payload.expires_at)),
        observed_at,
        payload_hash: String(payload.payload_hash),
        reporter_peer_id: String(payload.reporter_peer_id),
        reporter_server_public_key: String(payload.reporter_server_public_key),
        score: Number(payload.score),
        signature: String(payload.signature),
        target_peer_id: String(payload.target_peer_id),
      },
      create: {
        expires_at: new Date(String(payload.expires_at)),
        id: row.primary_key,
        observed_at,
        payload_hash: String(payload.payload_hash),
        reporter_peer_id: String(payload.reporter_peer_id),
        reporter_server_public_key: String(payload.reporter_server_public_key),
        score: Number(payload.score),
        signature: String(payload.signature),
        target_peer_id: String(payload.target_peer_id),
      },
    });
  }

  private async apply_server_score_aggregate(row: SyncRowEnvelope) {
    const payload = row.payload;
    const existing = await this.prisma_service.serverScoreAggregate.findUnique({
      where: {
        target_peer_id: row.primary_key,
      },
      select: {
        updated_at: true,
      },
    });

    if (
      existing &&
      existing.updated_at.getTime() > new Date(row.updated_at).getTime()
    ) {
      return;
    }

    await this.prisma_service.serverScoreAggregate.upsert({
      where: {
        target_peer_id: row.primary_key,
      },
      update: {
        last_report_at: new Date(String(payload.last_report_at)),
        mean_score: Number(payload.mean_score),
        report_count: Number(payload.report_count),
        updated_at: new Date(String(payload.updated_at)),
      },
      create: {
        last_report_at: new Date(String(payload.last_report_at)),
        mean_score: Number(payload.mean_score),
        report_count: Number(payload.report_count),
        target_peer_id: row.primary_key,
        updated_at: new Date(String(payload.updated_at)),
      },
    });
  }

  private async apply_user(row: SyncRowEnvelope) {
    const payload = row.payload;
    const existing = await this.prisma_service.user.findUnique({
      where: {
        public_key: row.primary_key,
      },
      select: {
        updatedAt: true,
      },
    });

    if (
      existing &&
      existing.updatedAt.getTime() > new Date(row.updated_at).getTime()
    ) {
      return;
    }

    await this.prisma_service.user.upsert({
      where: {
        public_key: row.primary_key,
      },
      update: {
        email: payload.email ? String(payload.email) : null,
        name: payload.name ? String(payload.name) : null,
        phone: payload.phone ? String(payload.phone) : null,
        quote: payload.quote ? String(payload.quote) : null,
        username: payload.username ? String(payload.username) : null,
        verfiication_id: payload.verfiication_id
          ? String(payload.verfiication_id)
          : null,
        x25519_public_key: payload.x25519_public_key
          ? String(payload.x25519_public_key)
          : null,
      },
      create: {
        email: payload.email ? String(payload.email) : null,
        name: payload.name ? String(payload.name) : null,
        phone: payload.phone ? String(payload.phone) : null,
        public_key: row.primary_key,
        quote: payload.quote ? String(payload.quote) : null,
        username: payload.username ? String(payload.username) : null,
        verfiication_id: payload.verfiication_id
          ? String(payload.verfiication_id)
          : null,
        x25519_public_key: payload.x25519_public_key
          ? String(payload.x25519_public_key)
          : null,
      },
    });
  }

  private async apply_direct_message(row: SyncRowEnvelope) {
    const payload = row.payload;
    const existing = await this.prisma_service.directMessage.findUnique({
      where: {
        id: row.primary_key,
      },
      select: {
        createdAt: true,
      },
    });

    if (
      existing &&
      existing.createdAt.getTime() > new Date(row.updated_at).getTime()
    ) {
      return;
    }

    await this.prisma_service.directMessage.upsert({
      where: {
        id: row.primary_key,
      },
      update: {
        algorithm: String(payload.algorithm),
        expiresAt: new Date(String(payload.expires_at)),
        message: String(payload.message),
        message_hash: String(payload.message_hash),
        recipient_public_key: String(payload.recipient_public_key),
        send_time: new Date(String(payload.send_time)),
        sender_public_key: String(payload.sender_public_key),
        sender_signature: String(payload.sender_signature),
        sender_x25519_public_key: String(payload.sender_x25519_public_key),
      },
      create: {
        algorithm: String(payload.algorithm),
        expiresAt: new Date(String(payload.expires_at)),
        id: row.primary_key,
        message: String(payload.message),
        message_hash: String(payload.message_hash),
        recipient_public_key: String(payload.recipient_public_key),
        send_time: new Date(String(payload.send_time)),
        sender_public_key: String(payload.sender_public_key),
        sender_signature: String(payload.sender_signature),
        sender_x25519_public_key: String(payload.sender_x25519_public_key),
      },
    });
  }
}
