import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { get_p2p_config } from '../../../../config/p2p.config';
import {
  PEER_DB_SYNC_FETCH_BY_KEYS_PROTOCOL,
  PEER_DB_SYNC_FETCH_PROTOCOL,
  PEER_DB_SYNC_MANIFEST_PROTOCOL,
  PEER_DB_SYNC_VERIFY_PROTOCOL,
} from '../../constants/peer_protocol.constants';
import { SyncProgressLoggerService } from './sync_progress_logger.service';
import { TableSyncRunnerService } from './table_sync_runner.service';
import {
  ValidatorDiscoveryService,
  SyncValidator,
} from './validator_discovery.service';
import {
  SYNC_TABLE_ORDER,
  SyncFetchByKeysRequest,
  SyncFetchByKeysResponse,
  SyncFetchRequest,
  SyncFetchResponse,
  SyncManifestRequest,
  SyncManifestResponse,
  SyncRowEnvelope,
  SyncTableName,
  SyncVerifyBatchRequest,
  SyncVerifyBatchResponse,
} from '../types/sync_wire.types';
import {
  RowConsensusResult,
  VoteVerificationService,
} from './vote_verification.service';

type StartupSyncRuntime = {
  connected_peer_ids: string[];
  local_peer_id: string;
  request_peer: (
    peer_id: string,
    protocol: string,
    payload: unknown,
  ) => Promise<unknown>;
};

type ManifestPickResult =
  | {
      manifest: SyncManifestResponse;
      validator: SyncValidator;
    }
  | {
      errors: string[];
      manifest: null;
      validator: null;
    };

@Injectable()
export class StartupSyncService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StartupSyncService.name);
  private readonly p2p_config = get_p2p_config();
  private readonly weak_consensus_queue = new Map<
    string,
    {
      enqueued_at: string;
      primary_key: string;
      table: SyncTableName;
    }
  >();
  private reverify_timer: NodeJS.Timeout | null = null;
  private is_sync_running = false;
  private queued_run_requested = false;

  constructor(
    private readonly sync_progress_logger_service: SyncProgressLoggerService,
    private readonly table_sync_runner_service: TableSyncRunnerService,
    private readonly validator_discovery_service: ValidatorDiscoveryService,
    private readonly vote_verification_service: VoteVerificationService,
  ) {}

  onModuleInit() {
    this.reverify_timer = setInterval(() => {
      if (this.weak_consensus_queue.size > 0) {
        this.logger.warn(
          `[SYNC][REVERIFY] queued_rows=${this.weak_consensus_queue.size}`,
        );
      }
    }, this.p2p_config.sync_reverify_interval_ms);
  }

  onModuleDestroy() {
    if (this.reverify_timer) {
      clearInterval(this.reverify_timer);
    }
  }

  async run_sync(
    reason: 'peer_reconnect' | 'startup',
    runtime: StartupSyncRuntime,
    wait_for_completion: boolean,
  ) {
    if (!this.p2p_config.sync_enabled) {
      return;
    }

    const run = async () => {
      if (this.is_sync_running) {
        this.queued_run_requested = true;
        return;
      }

      this.is_sync_running = true;

      try {
        await this.execute_sync(reason, runtime);

        while (this.queued_run_requested) {
          this.queued_run_requested = false;
          await this.execute_sync('peer_reconnect', runtime);
        }
      } catch (error) {
        this.logger.warn(
          `[SYNC] run failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      } finally {
        this.is_sync_running = false;
      }
    };

    if (wait_for_completion) {
      await run();
      return;
    }

    void run();
  }

  private async execute_sync(
    reason: 'peer_reconnect' | 'startup',
    runtime: StartupSyncRuntime,
  ) {
    const run_id = this.sync_progress_logger_service.begin_run(
      reason,
      SYNC_TABLE_ORDER.length + 1,
    );
    const validators =
      await this.validator_discovery_service.discover_validators({
        connected_peer_ids: runtime.connected_peer_ids,
        local_peer_id: runtime.local_peer_id,
        target_count: this.p2p_config.sync_validator_target,
      });

    if (validators.length === 0) {
      this.sync_progress_logger_service.log_step_end(
        run_id,
        1,
        'validator_discovery',
        'SKIPPED',
        {
          reason: 'No connected validators are available.',
        },
      );
      this.sync_progress_logger_service.end_run(run_id, 'SKIPPED');
      return;
    }

    const source_validator = validators[0];
    this.sync_progress_logger_service.log_step_start(
      run_id,
      1,
      'validator_discovery',
    );
    this.sync_progress_logger_service.log_step_end(
      run_id,
      1,
      'validator_discovery',
      'DONE',
      {
        selected_validator_count: validators.length,
        source_peer_id: source_validator.peer_id,
      },
    );

    const manifest_result = await this.pick_manifest_source_validator({
      run_id,
      runtime,
      validators,
    });

    if (!manifest_result.manifest || !manifest_result.validator) {
      this.sync_progress_logger_service.end_run(run_id, 'FAILED', {
        errors: manifest_result.errors,
        reason: 'No validator responded to sync manifest request.',
      });
      return;
    }

    const manifest = manifest_result.manifest;
    const effective_source_validator = manifest_result.validator;
    let partial_failure = false;

    for (
      let table_index = 0;
      table_index < SYNC_TABLE_ORDER.length;
      table_index += 1
    ) {
      const table = SYNC_TABLE_ORDER[table_index];
      const step_index = table_index + 2;
      const table_total_count =
        manifest?.tables.find((item) => item.table === table)?.row_count ?? 0;
      const table_sync_promise = this.sync_table({
        run_id,
        runtime,
        source_validator: effective_source_validator,
        table,
        table_total_count,
        validators,
      });

      try {
        await this.with_timeout(
          table_sync_promise,
          this.p2p_config.sync_table_timeout_ms,
        );
      } catch (error) {
        partial_failure = true;
        this.sync_progress_logger_service.log_step_end(
          run_id,
          step_index,
          table,
          'TIMEOUT',
          {
            error: error instanceof Error ? error.message : String(error),
          },
        );
      }
    }

    this.sync_progress_logger_service.end_run(
      run_id,
      partial_failure ? 'PARTIAL' : 'DONE',
      {
        weak_consensus_queue_count: this.weak_consensus_queue.size,
      },
    );
  }

  private async fetch_manifest_from_source(
    run_id: string,
    source_peer_id: string,
    runtime: StartupSyncRuntime,
  ) {
    const request: SyncManifestRequest = {
      run_id,
      type: 'sync.manifest.request',
    };
    const response = await runtime.request_peer(
      source_peer_id,
      PEER_DB_SYNC_MANIFEST_PROTOCOL,
      request,
    );

    if (!this.is_sync_manifest_response(response)) {
      return null;
    }

    return response;
  }

  private async pick_manifest_source_validator(input: {
    run_id: string;
    runtime: StartupSyncRuntime;
    validators: SyncValidator[];
  }): Promise<ManifestPickResult> {
    const errors: string[] = [];

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      for (const validator of input.validators) {
        try {
          const manifest = await this.fetch_manifest_from_source(
            input.run_id,
            validator.peer_id,
            input.runtime,
          );

          if (manifest) {
            return {
              manifest,
              validator,
            };
          }

          errors.push(
            `validator=${validator.peer_id} attempt=${attempt} invalid_manifest_response`,
          );
        } catch (error) {
          errors.push(
            `validator=${validator.peer_id} attempt=${attempt} error=${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }
      }

      await this.sleep(300 * attempt);
    }

    return {
      errors,
      manifest: null,
      validator: null,
    };
  }

  private async sync_table(input: {
    run_id: string;
    runtime: StartupSyncRuntime;
    source_validator: SyncValidator;
    table: SyncTableName;
    table_total_count: number;
    validators: SyncValidator[];
  }) {
    const step_index = SYNC_TABLE_ORDER.indexOf(input.table) + 2;
    const started_at_ms = Date.now();

    this.sync_progress_logger_service.log_step_start(
      input.run_id,
      step_index,
      input.table,
    );

    let cursor: string | null = null;
    let processed_count = 0;

    while (true) {
      const fetch_request: SyncFetchRequest = {
        cursor,
        limit: this.p2p_config.sync_batch_size,
        run_id: input.run_id,
        table: input.table,
        type: 'sync.fetch.request',
      };
      const fetch_response = await input.runtime.request_peer(
        input.source_validator.peer_id,
        PEER_DB_SYNC_FETCH_PROTOCOL,
        fetch_request,
      );

      if (!this.is_sync_fetch_response(fetch_response, input.table)) {
        throw new Error(
          `Invalid sync.fetch response for table ${input.table} from ${input.source_validator.peer_id}.`,
        );
      }

      if (fetch_response.items.length === 0) {
        break;
      }

      const votes = await this.collect_votes(
        input.run_id,
        input.table,
        fetch_response.items,
        input.validators,
        input.runtime,
      );
      const consensus = this.vote_verification_service.choose_hashes(
        fetch_response.items,
        votes,
      );
      const resolved_rows = await this.resolve_rows_from_consensus(
        input.run_id,
        input.table,
        fetch_response.items,
        consensus,
        input.validators,
        input.runtime,
      );

      await this.table_sync_runner_service.apply_rows(resolved_rows);
      processed_count += resolved_rows.length;
      this.sync_progress_logger_service.log_step_progress(
        input.run_id,
        step_index,
        input.table,
        processed_count,
        input.table_total_count,
        Date.now() - started_at_ms,
      );
      cursor = fetch_response.next_cursor;

      if (!cursor) {
        break;
      }
    }

    this.sync_progress_logger_service.log_step_end(
      input.run_id,
      step_index,
      input.table,
      'DONE',
      {
        processed_count,
      },
    );
  }

  private async collect_votes(
    run_id: string,
    table: SyncTableName,
    rows: SyncRowEnvelope[],
    validators: SyncValidator[],
    runtime: StartupSyncRuntime,
  ) {
    const base_vote_rows = rows.map((row) => ({
      canonical_hash: row.canonical_hash,
      primary_key: row.primary_key,
    }));
    const vote_results: Array<{
      peer_id: string;
      votes: Map<string, string | null>;
      weight: number;
    }> = [];

    for (const validator of validators) {
      const request: SyncVerifyBatchRequest = {
        rows: base_vote_rows,
        run_id,
        table,
        type: 'sync.verify.request',
      };

      try {
        const response = await runtime.request_peer(
          validator.peer_id,
          PEER_DB_SYNC_VERIFY_PROTOCOL,
          request,
        );

        if (!this.is_sync_verify_response(response, table)) {
          continue;
        }

        vote_results.push({
          peer_id: validator.peer_id,
          votes: new Map(
            response.votes.map((vote) => [
              vote.primary_key,
              vote.canonical_hash,
            ]),
          ),
          weight: validator.weight,
        });
      } catch {
        continue;
      }
    }

    return vote_results;
  }

  private async resolve_rows_from_consensus(
    run_id: string,
    table: SyncTableName,
    source_rows: SyncRowEnvelope[],
    consensus: RowConsensusResult[],
    validators: SyncValidator[],
    runtime: StartupSyncRuntime,
  ) {
    const source_map = new Map(
      source_rows.map((row) => [row.primary_key, row]),
    );
    const result_rows: SyncRowEnvelope[] = [];

    for (const row_consensus of consensus) {
      const source_row = source_map.get(row_consensus.primary_key);

      if (!source_row) {
        continue;
      }

      if (
        !row_consensus.chosen_hash ||
        row_consensus.chosen_hash === source_row.canonical_hash
      ) {
        if (row_consensus.weak_consensus) {
          this.enqueue_weak_consensus_row(table, row_consensus.primary_key);
        }

        result_rows.push(source_row);
        continue;
      }

      const resolved_row = await this.fetch_row_from_supporters(
        run_id,
        table,
        row_consensus.primary_key,
        row_consensus.chosen_hash,
        row_consensus.supporting_peer_ids,
        validators,
        runtime,
      );

      if (!resolved_row) {
        this.enqueue_weak_consensus_row(table, row_consensus.primary_key);
        result_rows.push(source_row);
        continue;
      }

      if (row_consensus.weak_consensus) {
        this.enqueue_weak_consensus_row(table, row_consensus.primary_key);
      }

      result_rows.push(resolved_row);
    }

    return result_rows;
  }

  private async fetch_row_from_supporters(
    run_id: string,
    table: SyncTableName,
    key: string,
    expected_hash: string,
    supporting_peer_ids: string[],
    validators: SyncValidator[],
    runtime: StartupSyncRuntime,
  ) {
    const preferred_peer_ids =
      supporting_peer_ids.length > 0
        ? supporting_peer_ids
        : validators.map((validator) => validator.peer_id);

    for (const peer_id of preferred_peer_ids) {
      const request: SyncFetchByKeysRequest = {
        keys: [key],
        run_id,
        table,
        type: 'sync.fetch_by_keys.request',
      };

      try {
        const response = await runtime.request_peer(
          peer_id,
          PEER_DB_SYNC_FETCH_BY_KEYS_PROTOCOL,
          request,
        );

        if (!this.is_sync_fetch_by_keys_response(response, table)) {
          continue;
        }

        const row = response.items[0];

        if (!row) {
          continue;
        }

        if (row.canonical_hash === expected_hash) {
          return row;
        }
      } catch {
        continue;
      }
    }

    return null;
  }

  private enqueue_weak_consensus_row(
    table: SyncTableName,
    primary_key: string,
  ) {
    this.weak_consensus_queue.set(`${table}:${primary_key}`, {
      enqueued_at: new Date().toISOString(),
      primary_key,
      table,
    });
  }

  private with_timeout<T>(promise: Promise<T>, timeout_ms: number) {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Timed out after ${timeout_ms}ms.`));
      }, timeout_ms);

      promise
        .then((value) => {
          clearTimeout(timer);
          resolve(value);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private sleep(ms: number) {
    return new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  private is_sync_manifest_response(
    value: unknown,
  ): value is SyncManifestResponse {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.manifest.response' &&
      Array.isArray(candidate.tables)
    );
  }

  private is_sync_fetch_response(
    value: unknown,
    table: SyncTableName,
  ): value is SyncFetchResponse {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.fetch.response' &&
      candidate.table === table &&
      Array.isArray(candidate.items)
    );
  }

  private is_sync_verify_response(
    value: unknown,
    table: SyncTableName,
  ): value is SyncVerifyBatchResponse {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.verify.response' &&
      candidate.table === table &&
      Array.isArray(candidate.votes)
    );
  }

  private is_sync_fetch_by_keys_response(
    value: unknown,
    table: SyncTableName,
  ): value is SyncFetchByKeysResponse {
    if (typeof value !== 'object' || value == null) {
      return false;
    }

    const candidate = value as Record<string, unknown>;

    return (
      candidate.type === 'sync.fetch_by_keys.response' &&
      candidate.table === table &&
      Array.isArray(candidate.items)
    );
  }
}
