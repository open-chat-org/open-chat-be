import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../../prisma/prisma.service';
import { get_p2p_config } from '../../../../config/p2p.config';

export type SyncValidator = {
  mean_score: number;
  peer_id: string;
  weight: number;
};

type DiscoverValidatorsInput = {
  connected_peer_ids: string[];
  local_peer_id: string;
  target_count?: number;
};

@Injectable()
export class ValidatorDiscoveryService {
  private readonly p2p_config = get_p2p_config();

  constructor(private readonly prisma_service: PrismaService) {}

  async discover_validators(input: DiscoverValidatorsInput) {
    const target_count = Math.max(
      1,
      input.target_count ?? this.p2p_config.sync_validator_target,
    );
    const candidate_peer_ids = Array.from(
      new Set(input.connected_peer_ids),
    ).filter((peer_id) => peer_id !== input.local_peer_id);

    if (candidate_peer_ids.length === 0) {
      return [] as SyncValidator[];
    }

    const [aggregate_rows, fallback_node_rows] = await Promise.all([
      this.prisma_service.serverScoreAggregate.findMany({
        where: {
          target_peer_id: {
            in: candidate_peer_ids,
          },
        },
      }),
      this.prisma_service.serverNode.findMany({
        where: {
          peer_id: {
            in: candidate_peer_ids,
          },
        },
        select: {
          peer_id: true,
        },
      }),
    ]);
    const aggregate_map = new Map(
      aggregate_rows.map((row) => [row.target_peer_id, row]),
    );

    const validators = fallback_node_rows
      .map((row): SyncValidator => {
        const aggregate = aggregate_map.get(row.peer_id);
        const mean_score = aggregate?.mean_score ?? 50;
        const weight = Math.max(1, Math.min(100, Math.round(mean_score)));

        return {
          mean_score,
          peer_id: row.peer_id,
          weight,
        };
      })
      .sort((left, right) => {
        if (right.weight !== left.weight) {
          return right.weight - left.weight;
        }

        return left.peer_id.localeCompare(right.peer_id);
      });

    return validators.slice(0, target_count);
  }
}
