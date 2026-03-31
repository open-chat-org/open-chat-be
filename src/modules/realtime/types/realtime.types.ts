export type AuthChallengeRecord = {
  challenge_id: string;
  created_at: string;
  expires_at: string;
  nonce: string;
  public_key: string;
};

export type AuthConnectPayload = {
  challenge_id: string;
  last_session_id?: string;
  public_key: string;
  signature: string;
};

export type DeliveryAckPayload = {
  event_id: string;
  received_at: string;
  status: 'received';
};

export type ReliableEnvelope<Payload = unknown> = {
  ack_timeout_ms: number;
  attempt: number;
  event_id: string;
  payload: Payload;
  requires_ack: boolean;
  room_id?: string;
  sent_at: string;
  type: string;
};

export type ReliableEnvelopeInput<Payload = unknown> = {
  ack_timeout_ms?: number;
  event_id?: string;
  payload: Payload;
  requires_ack?: boolean;
  room_id?: string;
  type: string;
};

export type PendingEnvelopeRecord<Payload = unknown> = ReliableEnvelope<Payload>;

export type RoomControlMessage = {
  action: 'join' | 'leave' | 'replace';
  room_name?: string;
  rooms?: string[];
  session_id: string;
};

export type RoomFanoutMessage<Payload = unknown> = {
  envelope: ReliableEnvelope<Payload>;
  origin_node_id: string;
  room_name: string;
};

export type RealtimeSessionRecord = {
  connected_at: string;
  last_seen_at: string;
  node_id: string;
  public_key: string;
  reconnect_expires_at: string;
  session_id: string;
  status: 'active' | 'disconnected';
};

export type SystemFrame<Payload = Record<string, unknown>> = {
  data: Payload;
  type: 'system.connected' | 'system.error' | 'system.retry_exhausted';
};
