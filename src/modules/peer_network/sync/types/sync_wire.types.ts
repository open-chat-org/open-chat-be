export const SYNC_TABLE_ORDER = [
  'server_node',
  'server_score_report',
  'server_score_aggregate',
  'user',
  'direct_message',
] as const;

export type SyncTableName = (typeof SYNC_TABLE_ORDER)[number];

export type SyncRowEnvelope = {
  canonical_hash: string;
  payload: Record<string, unknown>;
  primary_key: string;
  table: SyncTableName;
  updated_at: string;
};

export type SyncManifestRequest = {
  run_id: string;
  type: 'sync.manifest.request';
};

export type SyncManifestResponse = {
  generated_at: string;
  local_peer_id: string;
  run_id: string;
  tables: Array<{
    row_count: number;
    table: SyncTableName;
  }>;
  type: 'sync.manifest.response';
};

export type SyncFetchRequest = {
  cursor?: string | null;
  limit: number;
  run_id: string;
  table: SyncTableName;
  type: 'sync.fetch.request';
};

export type SyncFetchResponse = {
  items: SyncRowEnvelope[];
  next_cursor: string | null;
  run_id: string;
  table: SyncTableName;
  type: 'sync.fetch.response';
};

export type SyncVerifyBatchRequest = {
  rows: Array<{
    canonical_hash: string;
    primary_key: string;
  }>;
  run_id: string;
  table: SyncTableName;
  type: 'sync.verify.request';
};

export type SyncVerifyBatchResponse = {
  run_id: string;
  table: SyncTableName;
  type: 'sync.verify.response';
  votes: Array<{
    canonical_hash: string | null;
    primary_key: string;
  }>;
};

export type SyncFetchByKeysRequest = {
  keys: string[];
  run_id: string;
  table: SyncTableName;
  type: 'sync.fetch_by_keys.request';
};

export type SyncFetchByKeysResponse = {
  items: SyncRowEnvelope[];
  run_id: string;
  table: SyncTableName;
  type: 'sync.fetch_by_keys.response';
};

export type DmDeleteGossipEvent = {
  acked_at: string;
  event_id: string;
  hop_count: number;
  max_hops: number;
  message_id: string;
  origin_peer_id: string;
  recipient_public_key: string;
  reporter_server_public_key: string;
  signature: string;
  type: 'dm.delete.gossip';
};

export type SyncProtocolPayload =
  | DmDeleteGossipEvent
  | SyncFetchByKeysRequest
  | SyncFetchByKeysResponse
  | SyncFetchRequest
  | SyncFetchResponse
  | SyncManifestRequest
  | SyncManifestResponse
  | SyncVerifyBatchRequest
  | SyncVerifyBatchResponse;
