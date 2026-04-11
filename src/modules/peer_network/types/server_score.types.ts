export type ServerScoreReportPayload = {
  expires_at: string;
  observed_at: string;
  payload_hash: string;
  reporter_peer_id: string;
  reporter_server_public_key: string;
  score: number;
  signature: string;
  target_peer_id: string;
  type: 'server.score.report';
};

export type ServerScoreReportValidationResult = {
  error?: string;
  ok: boolean;
  payload?: ServerScoreReportPayload;
};

export type ServerCandidate = {
  is_active: boolean;
  last_seen_at: string;
  listen_addresses: string[];
  mean_score: number;
  peer_id: string;
  report_count: number;
  server_public_key: string | null;
};
