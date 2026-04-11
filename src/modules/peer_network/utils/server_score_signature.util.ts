import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { createHash } from 'node:crypto';
import { ServerScoreReportPayload } from '../types/server_score.types';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

type CanonicalServerScorePayload = {
  expires_at: string;
  observed_at: string;
  reporter_peer_id: string;
  reporter_server_public_key: string;
  score: number;
  target_peer_id: string;
};

function remove_hex_prefix(value: string) {
  return value.startsWith('0x') ? value.slice(2) : value;
}

function normalize_hex(value: string) {
  const trimmed_value = value.trim().toLowerCase();
  return trimmed_value.startsWith('0x') ? trimmed_value : `0x${trimmed_value}`;
}

export function create_server_score_payload_hash(
  payload: CanonicalServerScorePayload,
) {
  const canonical_json = JSON.stringify({
    expires_at: payload.expires_at,
    observed_at: payload.observed_at,
    reporter_peer_id: payload.reporter_peer_id,
    reporter_server_public_key: payload.reporter_server_public_key,
    score: payload.score,
    target_peer_id: payload.target_peer_id,
  });
  const hash = createHash('sha256').update(canonical_json).digest('hex');

  return `0x${hash}`;
}

export async function create_server_score_signature(
  payload: CanonicalServerScorePayload,
  sign_message: (message: Uint8Array) => Promise<string>,
) {
  const payload_hash = create_server_score_payload_hash(payload);
  const signature = await sign_message(
    ed.etc.hexToBytes(remove_hex_prefix(payload_hash)),
  );

  return {
    payload_hash,
    signature: normalize_hex(signature),
  };
}

export async function verify_server_score_signature(
  payload: ServerScoreReportPayload,
) {
  const expected_hash = create_server_score_payload_hash({
    expires_at: payload.expires_at,
    observed_at: payload.observed_at,
    reporter_peer_id: payload.reporter_peer_id,
    reporter_server_public_key: payload.reporter_server_public_key,
    score: payload.score,
    target_peer_id: payload.target_peer_id,
  });

  if (normalize_hex(payload.payload_hash) !== expected_hash) {
    return false;
  }

  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(payload.signature)),
    ed.etc.hexToBytes(remove_hex_prefix(expected_hash)),
    ed.etc.hexToBytes(remove_hex_prefix(payload.reporter_server_public_key)),
  );
}
