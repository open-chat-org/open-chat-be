import { get_optional_env_number, get_optional_env_string } from './env.config';

export type RealtimeConfig = {
  ack_timeout_ms: number;
  auth_timeout_ms: number;
  challenge_ttl_seconds: number;
  max_retries: number;
  missed_pong_limit: number;
  ping_interval_ms: number;
  reconnect_grace_ms: number;
  retry_backoff_ms: number[];
  ws_path: string;
};

let realtime_config: RealtimeConfig | null = null;

export function get_realtime_config(): RealtimeConfig {
  if (realtime_config) {
    return realtime_config;
  }

  const ws_path = get_optional_env_string('WS_PATH', '/realtime') ?? '/realtime';
  const challenge_ttl_seconds = get_optional_env_number(
    'WS_CHALLENGE_TTL_SECONDS',
    60,
  );
  const reconnect_grace_ms = get_optional_env_number(
    'WS_RECONNECT_GRACE_MS',
    90_000,
  );
  const ping_interval_ms = get_optional_env_number(
    'WS_PING_INTERVAL_MS',
    25_000,
  );
  const missed_pong_limit = get_optional_env_number(
    'WS_MISSED_PONG_LIMIT',
    2,
  );
  const ack_timeout_ms = get_optional_env_number('WS_ACK_TIMEOUT_MS', 5_000);
  const max_retries = get_optional_env_number('WS_MAX_RETRIES', 5);

  realtime_config = {
    ack_timeout_ms,
    auth_timeout_ms: 10_000,
    challenge_ttl_seconds,
    max_retries,
    missed_pong_limit,
    ping_interval_ms,
    reconnect_grace_ms,
    retry_backoff_ms: [2_000, 4_000, 8_000, 16_000, 30_000],
    ws_path,
  };

  return realtime_config;
}
