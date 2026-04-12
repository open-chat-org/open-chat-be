import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { createHash } from 'node:crypto';
import { remove_hex_prefix } from '../../realtime/utils/realtime_signature.util';
import {
  SignedInterServerDmEvent,
  UnsignedInterServerDmEvent,
} from '../types/inter_server_dm.types';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

function normalize_hex(value: string) {
  const trimmed_value = value.trim().toLowerCase();
  return trimmed_value.startsWith('0x')
    ? trimmed_value
    : `0x${trimmed_value}`;
}

function create_event_signing_message(
  event: UnsignedInterServerDmEvent<unknown>,
) {
  const canonical_json = JSON.stringify({
    event_id: event.event_id,
    origin_peer_id: event.origin_peer_id,
    payload: event.payload,
    server_public_key: event.server_public_key,
    timestamp: event.timestamp,
    type: event.type,
  });
  const hash = createHash('sha256').update(canonical_json).digest('hex');

  return {
    hash: `0x${hash}`,
  };
}

export async function create_inter_server_dm_signature(input: {
  event: UnsignedInterServerDmEvent<unknown>;
  sign_message: (message: Uint8Array) => Promise<string>;
}) {
  const { hash } = create_event_signing_message(input.event);
  const signature = await input.sign_message(
    ed.etc.hexToBytes(remove_hex_prefix(hash)),
  );

  return {
    hash,
    signature: normalize_hex(signature),
  };
}

export async function verify_inter_server_dm_signature(
  event: SignedInterServerDmEvent<unknown>,
) {
  const { hash } = create_event_signing_message(event);

  return ed.verifyAsync(
    ed.etc.hexToBytes(remove_hex_prefix(event.server_signature)),
    ed.etc.hexToBytes(remove_hex_prefix(hash)),
    ed.etc.hexToBytes(remove_hex_prefix(event.server_public_key)),
  );
}
