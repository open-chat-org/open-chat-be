export type InterServerDmReplicatePayload = {
  algorithm: string;
  id: string;
  message: string;
  message_hash: string;
  origin_server_peer_id: string;
  recipient_public_key: string;
  replica_peer_ids: string[];
  send_time: string;
  sender_public_key: string;
  sender_signature: string;
  sender_x25519_public_key: string;
};

export type InterServerDmReplicateAckPayload = {
  id: string;
  stored: boolean;
};

export type InterServerDmPresenceAnnouncePayload = {
  expires_at: string;
  public_key: string;
  server_peer_id: string;
};

export type InterServerDmPresenceQueryPayload = {
  public_key: string;
};

export type InterServerDmPresenceResponsePayload = {
  online_server_peer_ids: string[];
  public_key: string;
};

export type InterServerDmPullRequestPayload = {
  cursor?: string | null;
  limit: number;
  public_key: string;
};

export type InterServerDmPullResponsePayload = {
  items: InterServerDmReplicatePayload[];
  next_cursor: string | null;
  public_key: string;
};

export type InterServerDmDeleteRequestPayload = {
  message_id: string;
  origin_server_peer_id: string;
  recipient_public_key: string;
  replica_peer_ids: string[];
};

export type InterServerDmDeleteAckPayload = {
  deleted: boolean;
  message_id: string;
};

export type SignedInterServerDmEvent<Payload> = {
  event_id: string;
  origin_peer_id: string;
  payload: Payload;
  server_public_key: string;
  server_signature: string;
  timestamp: string;
  type: string;
};

export type UnsignedInterServerDmEvent<Payload> = Omit<
  SignedInterServerDmEvent<Payload>,
  'server_signature'
>;

export type InterServerDmCallbacks = {
  on_delete_request: (
    payload: InterServerDmDeleteRequestPayload,
    source_peer_id: string,
  ) => Promise<InterServerDmDeleteAckPayload>;
  on_pull_request: (
    payload: InterServerDmPullRequestPayload,
    source_peer_id: string,
  ) => Promise<InterServerDmPullResponsePayload>;
  on_replicate_request: (
    payload: InterServerDmReplicatePayload,
    source_peer_id: string,
  ) => Promise<InterServerDmReplicateAckPayload>;
};

export type ReplicateToPeersInput = {
  payload: InterServerDmReplicatePayload;
  peer_ids: string[];
  require_quorum: boolean;
};

export type ReplicateToPeersResult = {
  acknowledged_peer_ids: string[];
  failed_peer_ids: string[];
  quorum_met: boolean;
};
