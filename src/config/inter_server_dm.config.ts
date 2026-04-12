import { get_optional_env_number } from './env.config';

export type InterServerDmConfig = {
  delete_max_retries: number;
  delete_retry_interval_ms: number;
  p2p_request_timeout_ms: number;
  presence_ttl_ms: number;
  pull_batch_size: number;
  replica_remote_count: number;
  replica_remote_quorum: number;
};

let inter_server_dm_config: InterServerDmConfig | null = null;

export function get_inter_server_dm_config(): InterServerDmConfig {
  if (inter_server_dm_config) {
    return inter_server_dm_config;
  }

  const replica_remote_count = Math.max(
    1,
    Math.trunc(get_optional_env_number('DM_REPLICA_REMOTE_COUNT', 3)),
  );
  const replica_remote_quorum = Math.min(
    replica_remote_count,
    Math.max(
      1,
      Math.trunc(get_optional_env_number('DM_REPLICA_REMOTE_QUORUM', 2)),
    ),
  );

  inter_server_dm_config = {
    delete_max_retries: Math.max(
      1,
      Math.trunc(get_optional_env_number('DM_DELETE_MAX_RETRIES', 12)),
    ),
    delete_retry_interval_ms: Math.max(
      1_000,
      Math.trunc(
        get_optional_env_number('DM_DELETE_RETRY_INTERVAL_MS', 10_000),
      ),
    ),
    p2p_request_timeout_ms: Math.max(
      1_000,
      Math.trunc(get_optional_env_number('DM_P2P_REQUEST_TIMEOUT_MS', 8_000)),
    ),
    presence_ttl_ms: Math.max(
      5_000,
      Math.trunc(get_optional_env_number('DM_PRESENCE_TTL_MS', 45_000)),
    ),
    pull_batch_size: Math.max(
      1,
      Math.trunc(get_optional_env_number('DM_PULL_BATCH_SIZE', 100)),
    ),
    replica_remote_count,
    replica_remote_quorum,
  };

  return inter_server_dm_config;
}

