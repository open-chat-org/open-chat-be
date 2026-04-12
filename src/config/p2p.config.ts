import { get_optional_env_number, get_optional_env_string } from './env.config';

export type P2pConfig = {
  alpha: number;
  bootstrap: string[];
  dial_timeout_ms: number;
  enabled: boolean;
  k_bucket_size: number;
  keep_alive_core_count: number;
  keep_alive_enabled: boolean;
  keep_alive_reconcile_ms: number;
  keep_alive_redial_ms: number;
  listen: string;
  max_parallel_reconnects: number;
  reconnect_backoff_factor: number;
  reconnect_retries: number;
  reconnect_retry_interval_ms: number;
  sync_batch_size: number;
  sync_dedupe_ttl_seconds: number;
  sync_enabled: boolean;
  sync_reverify_interval_ms: number;
  sync_startup_timeout_ms: number;
  sync_table_timeout_ms: number;
  sync_validator_target: number;
  score_default_max_report_age_seconds: number;
  score_gossip_interval_ms: number;
  score_report_ttl_seconds: number;
  trace_buffer_size: number;
  trace_max_payload_bytes: number;
  target_peers: number;
};

function parse_boolean(value: string | undefined, default_value: boolean) {
  if (!value) {
    return default_value;
  }

  const normalized_value = value.trim().toLowerCase();

  return normalized_value === 'true' || normalized_value === '1';
}

function parse_bootstrap_list(value: string | undefined): string[] {
  if (!value) {
    return [];
  }

  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

export function get_p2p_config(): P2pConfig {
  return {
    alpha: get_optional_env_number('P2P_ALPHA', 3),
    bootstrap: parse_bootstrap_list(get_optional_env_string('P2P_BOOTSTRAP')),
    dial_timeout_ms: get_optional_env_number('P2P_DIAL_TIMEOUT_MS', 15_000),
    enabled: parse_boolean(get_optional_env_string('P2P_ENABLED'), false),
    k_bucket_size: get_optional_env_number('P2P_K_BUCKET_SIZE', 20),
    keep_alive_core_count: get_optional_env_number(
      'P2P_KEEP_ALIVE_CORE_COUNT',
      8,
    ),
    keep_alive_enabled: parse_boolean(
      get_optional_env_string('P2P_KEEP_ALIVE_ENABLED'),
      true,
    ),
    keep_alive_reconcile_ms: get_optional_env_number(
      'P2P_KEEP_ALIVE_RECONCILE_MS',
      15_000,
    ),
    keep_alive_redial_ms: get_optional_env_number(
      'P2P_KEEP_ALIVE_REDIAL_MS',
      20_000,
    ),
    listen:
      get_optional_env_string('P2P_LISTEN', '/ip4/0.0.0.0/tcp/4101') ??
      '/ip4/0.0.0.0/tcp/4101',
    max_parallel_reconnects: get_optional_env_number(
      'P2P_MAX_PARALLEL_RECONNECTS',
      8,
    ),
    reconnect_backoff_factor: get_optional_env_number(
      'P2P_RECONNECT_BACKOFF_FACTOR',
      1.8,
    ),
    reconnect_retries: get_optional_env_number('P2P_RECONNECT_RETRIES', 20),
    reconnect_retry_interval_ms: get_optional_env_number(
      'P2P_RECONNECT_RETRY_INTERVAL_MS',
      1_000,
    ),
    sync_batch_size: get_optional_env_number('P2P_SYNC_BATCH_SIZE', 100),
    sync_dedupe_ttl_seconds: get_optional_env_number(
      'P2P_SYNC_DEDUPE_TTL_SECONDS',
      600,
    ),
    sync_enabled: parse_boolean(
      get_optional_env_string('P2P_SYNC_ENABLED'),
      true,
    ),
    sync_reverify_interval_ms: get_optional_env_number(
      'P2P_SYNC_REVERIFY_INTERVAL_MS',
      60_000,
    ),
    sync_startup_timeout_ms: get_optional_env_number(
      'P2P_SYNC_STARTUP_TIMEOUT_MS',
      30_000,
    ),
    sync_table_timeout_ms: get_optional_env_number(
      'P2P_SYNC_TABLE_TIMEOUT_MS',
      12_000,
    ),
    sync_validator_target: get_optional_env_number(
      'P2P_SYNC_VALIDATOR_TARGET',
      5,
    ),
    score_default_max_report_age_seconds: get_optional_env_number(
      'P2P_SCORE_DEFAULT_MAX_REPORT_AGE_SECONDS',
      180,
    ),
    score_gossip_interval_ms: get_optional_env_number(
      'P2P_SCORE_GOSSIP_INTERVAL_MS',
      12_000,
    ),
    score_report_ttl_seconds: get_optional_env_number(
      'P2P_SCORE_REPORT_TTL_SECONDS',
      120,
    ),
    trace_buffer_size: get_optional_env_number('P2P_TRACE_BUFFER_SIZE', 5000),
    trace_max_payload_bytes: get_optional_env_number(
      'P2P_TRACE_MAX_PAYLOAD_BYTES',
      2048,
    ),
    target_peers: get_optional_env_number('P2P_TARGET_PEERS', 16),
  };
}
