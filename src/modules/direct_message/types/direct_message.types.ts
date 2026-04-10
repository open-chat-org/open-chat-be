export type ChatMessageSendPayload = {
  algorithm: string;
  id: string;
  message: string;
  message_hash: string;
  recipient_public_key: string;
  sender_signature: string;
  send_time: string;
};

export type ChatMessagePersistedPayload = {
  message_id: string;
};

export type ChatMessageEventPayload = {
  algorithm: string;
  expires_at: string;
  id: string;
  message: string;
  message_hash: string;
  send_time: string;
  sender_public_key: string;
  sender_signature: string;
  sender_x25519_public_key: string;
};

export type ChatMessageAcceptedPayload = {
  expires_at: string;
  id: string;
  recipient_public_key: string;
  send_time: string;
};
