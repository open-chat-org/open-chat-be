import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { NetworkTraceService } from '../../../network_trace/network_trace.service';
import { SyncTableName } from '../types/sync_wire.types';

type SyncStepStatus = 'DONE' | 'FAILED' | 'SKIPPED' | 'TIMEOUT';

type SyncRunSummary = {
  reason: 'peer_reconnect' | 'startup';
  run_id: string;
  started_at_ms: number;
  step_count: number;
};

@Injectable()
export class SyncProgressLoggerService {
  private readonly logger = new Logger(SyncProgressLoggerService.name);
  private readonly run_summaries = new Map<string, SyncRunSummary>();

  constructor(private readonly network_trace_service: NetworkTraceService) {}

  begin_run(reason: 'peer_reconnect' | 'startup', step_count: number) {
    const run_id = randomUUID();
    const started_at_ms = Date.now();

    this.run_summaries.set(run_id, {
      reason,
      run_id,
      started_at_ms,
      step_count,
    });
    this.logger.log(`[SYNC][${run_id}] RUN START reason=${reason}`);

    return run_id;
  }

  log_step_start(run_id: string, step_index: number, step_name: string) {
    this.logger.log(`[SYNC][${run_id}][${step_index}] ${step_name} START`);
  }

  log_step_progress(
    run_id: string,
    step_index: number,
    table: SyncTableName,
    processed: number,
    total: number,
    elapsed_ms: number,
  ) {
    const percent = total > 0 ? Math.min(100, (processed / total) * 100) : 100;

    this.logger.log(
      `[SYNC][${run_id}][${step_index}] ${table} PROGRESS ${processed}/${total} (${percent.toFixed(1)}%) elapsed=${elapsed_ms}ms`,
    );
  }

  log_step_end(
    run_id: string,
    step_index: number,
    step_name: string,
    status: SyncStepStatus,
    details?: Record<string, unknown>,
  ) {
    this.logger.log(`[SYNC][${run_id}][${step_index}] ${step_name} ${status}`);
    this.network_trace_service.record_event({
      details: {
        ...details,
        run_id,
        status,
        step_index,
        step_name,
      },
      event_type: 'p2p.sync_step_completed',
      severity: status === 'DONE' ? 'info' : 'warn',
      source: 'p2p',
    });
  }

  end_run(
    run_id: string,
    status: SyncStepStatus | 'PARTIAL',
    details?: Record<string, unknown>,
  ) {
    const summary = this.run_summaries.get(run_id);
    const elapsed_ms = summary ? Date.now() - summary.started_at_ms : undefined;

    const details_text =
      details && Object.keys(details).length > 0
        ? ` details=${JSON.stringify(details)}`
        : '';

    this.logger.log(
      `[SYNC][${run_id}] RUN ${status} elapsed=${elapsed_ms ?? 'n/a'}ms${details_text}`,
    );
    this.network_trace_service.record_event({
      details: {
        ...details,
        elapsed_ms,
        reason: summary?.reason,
        run_id,
        status,
      },
      event_type: 'p2p.sync_run_completed',
      severity: status === 'DONE' ? 'info' : 'warn',
      source: 'p2p',
    });
    this.run_summaries.delete(run_id);
  }
}
