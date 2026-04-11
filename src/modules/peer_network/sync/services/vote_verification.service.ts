import { Injectable } from '@nestjs/common';
import { SyncRowEnvelope } from '../types/sync_wire.types';

type ValidatorVoteResult = {
  peer_id: string;
  votes: Map<string, string | null>;
  weight: number;
};

export type RowConsensusResult = {
  chosen_hash: string;
  primary_key: string;
  supporting_peer_ids: string[];
  weak_consensus: boolean;
};

@Injectable()
export class VoteVerificationService {
  choose_hashes(
    rows: SyncRowEnvelope[],
    votes: ValidatorVoteResult[],
  ): RowConsensusResult[] {
    return rows.map((row) =>
      this.resolve_row_consensus(row.primary_key, votes),
    );
  }

  private resolve_row_consensus(
    primary_key: string,
    votes: ValidatorVoteResult[],
  ): RowConsensusResult {
    const hash_weight_map = new Map<string, number>();
    const supporters_map = new Map<string, string[]>();
    let responding_weight = 0;

    for (const vote_result of votes) {
      const hash = vote_result.votes.get(primary_key);

      if (!hash) {
        continue;
      }

      responding_weight += vote_result.weight;
      const current_weight = hash_weight_map.get(hash) ?? 0;
      hash_weight_map.set(hash, current_weight + vote_result.weight);
      const supporters = supporters_map.get(hash) ?? [];

      supporters.push(vote_result.peer_id);
      supporters_map.set(hash, supporters);
    }

    if (hash_weight_map.size === 0) {
      return {
        chosen_hash: '',
        primary_key,
        supporting_peer_ids: [],
        weak_consensus: true,
      };
    }

    const winner = Array.from(hash_weight_map.entries()).sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }

      return left[0].localeCompare(right[0]);
    })[0];
    const strict_majority =
      responding_weight > 0 && winner[1] > responding_weight / 2;

    return {
      chosen_hash: winner[0],
      primary_key,
      supporting_peer_ids: supporters_map.get(winner[0]) ?? [],
      weak_consensus: !strict_majority || votes.length <= 1,
    };
  }
}
