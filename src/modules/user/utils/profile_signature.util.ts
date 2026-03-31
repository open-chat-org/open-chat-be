import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { createHash } from 'node:crypto';
import { UpdateUserProfileDto } from '../dto/update_user_profile.dto';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

type ProfileSignaturePayload = {
  email: string;
  name: string;
  phone: string;
  quote: string;
  username: string;
};

function remove_hex_prefix(value: string) {
  return value.startsWith('0x') ? value.slice(2) : value;
}

function normalize_profile_value(value?: string) {
  return value?.trim() ?? '';
}

export function build_profile_signature_payload(
  payload: UpdateUserProfileDto,
): ProfileSignaturePayload {
  return {
    username: normalize_profile_value(payload.username),
    name: normalize_profile_value(payload.name),
    quote: normalize_profile_value(payload.quote),
    phone: normalize_profile_value(payload.phone),
    email: normalize_profile_value(payload.email),
  };
}

export function create_profile_signature_message(payload: ProfileSignaturePayload) {
  return JSON.stringify({
    username: payload.username,
    name: payload.name,
    quote: payload.quote,
    phone: payload.phone,
    email: payload.email,
  });
}

export function create_profile_hash(payload: ProfileSignaturePayload) {
  const message = create_profile_signature_message(payload);
  const message_hash = createHash('sha256').update(message).digest('hex');

  return `0x${message_hash}`;
}

export async function verify_profile_signature(
  public_key: string,
  payload: UpdateUserProfileDto,
) {
  const normalized_payload = build_profile_signature_payload(payload);
  const expected_hash = create_profile_hash(normalized_payload);

  if (expected_hash !== payload.profile_hash) {
    return false;
  }

  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(payload.profile_signature)),
    ed.etc.hexToBytes(remove_hex_prefix(payload.profile_hash)),
    ed.etc.hexToBytes(remove_hex_prefix(public_key)),
  );
}
