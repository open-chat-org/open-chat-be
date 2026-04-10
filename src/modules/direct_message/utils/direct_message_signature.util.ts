import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { BadRequestException } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { get_direct_message_config } from '../../../config/direct_message.config';
import {
  normalize_public_key,
  normalize_signature,
  remove_hex_prefix,
} from '../../realtime/utils/realtime_signature.util';
import {
  ChatMessagePersistedPayload,
  ChatMessageSendPayload,
} from '../types/direct_message.types';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

const direct_message_config = get_direct_message_config();
const uuid_v4_pattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type DirectMessageSignaturePayload = {
  id: string;
  message: string;
  send_time: string;
};

function normalize_message_hash(value: unknown) {
  if (typeof value !== 'string') {
    throw new BadRequestException(
      'message_hash must be provided as a string.',
    );
  }

  const message_hash = value.trim().startsWith('0x')
    ? value.trim()
    : `0x${value.trim()}`;

  if (!/^0x[a-fA-F0-9]{64}$/.test(message_hash)) {
    throw new BadRequestException(
      'message_hash must be a valid SHA-256 hex string.',
    );
  }

  return message_hash;
}

function normalize_message_id(value: unknown, field_name = 'id') {
  if (typeof value !== 'string') {
    throw new BadRequestException(`${field_name} must be a string.`);
  }

  const message_id = value.trim();

  if (!uuid_v4_pattern.test(message_id)) {
    throw new BadRequestException(`${field_name} must be a valid UUID v4.`);
  }

  return message_id;
}

function normalize_message(value: unknown) {
  if (typeof value !== 'string') {
    throw new BadRequestException('message must be provided as a string.');
  }

  if (!value) {
    throw new BadRequestException('message is required.');
  }

  return value;
}

function normalize_send_time(value: unknown) {
  if (typeof value !== 'string') {
    throw new BadRequestException('send_time must be a string.');
  }

  const send_time = value.trim();

  if (!send_time || Number.isNaN(Date.parse(send_time))) {
    throw new BadRequestException('send_time must be a valid ISO date string.');
  }

  return send_time;
}

function normalize_algorithm(value: unknown) {
  if (typeof value !== 'string') {
    throw new BadRequestException('algorithm must be a string.');
  }

  const algorithm = value.trim();

  if (algorithm !== direct_message_config.algorithm) {
    throw new BadRequestException('Unsupported direct-message algorithm.');
  }

  return algorithm;
}

export function build_direct_message_signature_payload(
  payload: Pick<ChatMessageSendPayload, 'id' | 'message' | 'send_time'>,
): DirectMessageSignaturePayload {
  return {
    id: normalize_message_id(payload.id),
    message: normalize_message(payload.message),
    send_time: normalize_send_time(payload.send_time),
  };
}

export function create_direct_message_signature_message(
  payload: DirectMessageSignaturePayload,
) {
  return JSON.stringify({
    message: payload.message,
    id: payload.id,
    send_time: payload.send_time,
  });
}

export function create_direct_message_hash(
  payload: DirectMessageSignaturePayload,
) {
  const message = create_direct_message_signature_message(payload);
  const message_hash = createHash('sha256').update(message).digest('hex');

  return `0x${message_hash}`;
}

export async function verify_direct_message_signature(
  public_key: string,
  payload: ChatMessageSendPayload,
) {
  const normalized_payload = build_direct_message_signature_payload(payload);
  const expected_hash = create_direct_message_hash(normalized_payload);

  if (expected_hash !== payload.message_hash) {
    return false;
  }

  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(payload.sender_signature)),
    ed.etc.hexToBytes(remove_hex_prefix(payload.message_hash)),
    ed.etc.hexToBytes(remove_hex_prefix(public_key)),
  );
}

export function parse_chat_message_send_payload(
  value: unknown,
): ChatMessageSendPayload {
  if (typeof value !== 'object' || value === null) {
    throw new BadRequestException('chat.message.send payload must be an object.');
  }

  const payload = value as Record<string, unknown>;

  return {
    algorithm: normalize_algorithm(payload.algorithm),
    id: normalize_message_id(payload.id),
    message: normalize_message(payload.message),
    message_hash: normalize_message_hash(payload.message_hash),
    recipient_public_key: normalize_public_key(payload.recipient_public_key),
    sender_signature: normalize_signature(payload.sender_signature),
    send_time: normalize_send_time(payload.send_time),
  };
}

export function parse_chat_message_persisted_payload(
  value: unknown,
): ChatMessagePersistedPayload {
  if (typeof value !== 'object' || value === null) {
    throw new BadRequestException(
      'chat.message.persisted payload must be an object.',
    );
  }

  const payload = value as Record<string, unknown>;

  return {
    message_id: normalize_message_id(payload.message_id, 'message_id'),
  };
}
