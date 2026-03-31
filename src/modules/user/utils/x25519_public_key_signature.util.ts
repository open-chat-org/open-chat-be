import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { createHash } from 'node:crypto';
import { RegisterX25519PublicKeyDto } from '../dto/register_x25519_public_key.dto';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

type X25519PublicKeySignaturePayload = {
  x25519_public_key: string;
};

function remove_hex_prefix(value: string) {
  return value.startsWith('0x') ? value.slice(2) : value;
}

function normalize_x25519_public_key(value: string) {
  const trimmed_value = value.trim();

  return trimmed_value.startsWith('0x') ? trimmed_value : `0x${trimmed_value}`;
}

export function build_x25519_public_key_signature_payload(
  payload: RegisterX25519PublicKeyDto,
): X25519PublicKeySignaturePayload {
  return {
    x25519_public_key: normalize_x25519_public_key(payload.x25519_public_key),
  };
}

export function create_x25519_public_key_signature_message(
  payload: X25519PublicKeySignaturePayload,
) {
  return JSON.stringify({
    x25519_public_key: payload.x25519_public_key,
  });
}

export function create_x25519_public_key_hash(
  payload: X25519PublicKeySignaturePayload,
) {
  const message = create_x25519_public_key_signature_message(payload);
  const message_hash = createHash('sha256').update(message).digest('hex');

  return `0x${message_hash}`;
}

export async function verify_x25519_public_key_signature(
  public_key: string,
  payload: RegisterX25519PublicKeyDto,
) {
  const normalized_payload = build_x25519_public_key_signature_payload(payload);
  const expected_hash = create_x25519_public_key_hash(normalized_payload);

  if (expected_hash !== payload.x25519_public_key_hash) {
    return false;
  }

  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(payload.x25519_public_key_signature)),
    ed.etc.hexToBytes(remove_hex_prefix(payload.x25519_public_key_hash)),
    ed.etc.hexToBytes(remove_hex_prefix(public_key)),
  );
}
