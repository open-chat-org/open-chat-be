import { get_optional_env_number, get_optional_env_string } from './env.config';

export type P2pConfig = {
  alpha: number;
  bootstrap: string[];
  enabled: boolean;
  k_bucket_size: number;
  listen: string;
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
    enabled: parse_boolean(get_optional_env_string('P2P_ENABLED'), false),
    k_bucket_size: get_optional_env_number('P2P_K_BUCKET_SIZE', 20),
    listen:
      get_optional_env_string('P2P_LISTEN', '/ip4/0.0.0.0/tcp/4101') ??
      '/ip4/0.0.0.0/tcp/4101',
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
