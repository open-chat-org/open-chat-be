import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { BadRequestException } from '@nestjs/common';
import { AuthConnectPayload } from '../types/realtime.types';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

export function remove_hex_prefix(value: string) {
  return value.startsWith('0x') ? value.slice(2) : value;
}

export function normalize_public_key(value: unknown): string {
  if (typeof value !== 'string') {
    throw new BadRequestException('public_key must be provided as a string.');
  }

  const trimmed_value = value.trim();
  const public_key = trimmed_value.startsWith('0x')
    ? trimmed_value
    : `0x${trimmed_value}`;

  if (!/^0x[a-fA-F0-9]{64,130}$/.test(public_key)) {
    throw new BadRequestException(
      'public_key must be a valid hex string starting with 0x and containing 64 to 130 hex characters.',
    );
  }

  return public_key;
}

export function normalize_signature(value: unknown): string {
  if (typeof value !== 'string') {
    throw new BadRequestException('signature must be provided as a string.');
  }

  const trimmed_value = value.trim();
  const signature = trimmed_value.startsWith('0x')
    ? trimmed_value
    : `0x${trimmed_value}`;

  if (!/^0x[a-fA-F0-9]{128}$/.test(signature)) {
    throw new BadRequestException(
      'signature must be a valid Ed25519 signature hex string.',
    );
  }

  return signature;
}

export function normalize_session_id(value: unknown) {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof value !== 'string') {
    throw new BadRequestException('last_session_id must be a string.');
  }

  const session_id = value.trim();

  if (!session_id) {
    return undefined;
  }

  return session_id;
}

export function create_auth_challenge_message(
  challenge_id: string,
  public_key: string,
  nonce: string,
  expires_at: string,
) {
  return `open-chat:ws-auth:${challenge_id}:${public_key}:${nonce}:${expires_at}`;
}

export async function verify_message_signature(
  public_key: string,
  signature: string,
  message: string,
) {
  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(signature)),
    new TextEncoder().encode(message),
    ed.etc.hexToBytes(remove_hex_prefix(public_key)),
  );
}

export function parse_auth_connect_payload(
  value: unknown,
): AuthConnectPayload {
  if (typeof value !== 'object' || value === null) {
    throw new BadRequestException('auth.connect payload must be an object.');
  }

  const payload = value as Record<string, unknown>;

  if (typeof payload.challenge_id !== 'string' || !payload.challenge_id.trim()) {
    throw new BadRequestException('challenge_id is required.');
  }

  return {
    challenge_id: payload.challenge_id.trim(),
    last_session_id: normalize_session_id(payload.last_session_id),
    public_key: normalize_public_key(payload.public_key),
    signature: normalize_signature(payload.signature),
  };
}
