import { Injectable } from '@nestjs/common';
import * as ed from '@noble/ed25519';
import { sha512 } from '@noble/hashes/sha2.js';
import { createHash, randomUUID } from 'node:crypto';
import { get_p2p_config } from '../../../../config/p2p.config';
import { DmDeleteGossipEvent } from '../types/sync_wire.types';

ed.hashes.sha512 = sha512;
ed.hashes.sha512Async = async (message: Uint8Array) => sha512(message);

function remove_hex_prefix(value: string) {
  return value.startsWith('0x') ? value.slice(2) : value;
}

function normalize_hex(value: string) {
  const trimmed = value.trim().toLowerCase();
  return trimmed.startsWith('0x') ? trimmed : `0x${trimmed}`;
}

@Injectable()
export class DmDeleteGossipService {
  private readonly p2p_config = get_p2p_config();
  private readonly dedupe_cache = new Map<string, number>();

  async create_event(input: {
    local_peer_id: string;
    max_hops: number;
    message_id: string;
    recipient_public_key: string;
    reporter_server_public_key: string;
    sign_message: (message: Uint8Array) => Promise<string>;
  }) {
    const unsigned_event = {
      acked_at: new Date().toISOString(),
      event_id: randomUUID(),
      hop_count: 0,
      max_hops: input.max_hops,
      message_id: input.message_id,
      origin_peer_id: input.local_peer_id,
      recipient_public_key: input.recipient_public_key,
      reporter_server_public_key: normalize_hex(
        input.reporter_server_public_key,
      ),
      type: 'dm.delete.gossip' as const,
    };
    const event_hash = this.create_event_hash(unsigned_event);
    const signature = normalize_hex(
      await input.sign_message(
        ed.etc.hexToBytes(remove_hex_prefix(event_hash)),
      ),
    );

    return {
      ...unsigned_event,
      signature,
    } as DmDeleteGossipEvent;
  }

  parse_event(value: unknown): DmDeleteGossipEvent | null {
    if (typeof value !== 'object' || value == null) {
      return null;
    }

    const candidate = value as Record<string, unknown>;

    if (candidate.type !== 'dm.delete.gossip') {
      return null;
    }

    if (
      typeof candidate.event_id !== 'string' ||
      typeof candidate.message_id !== 'string' ||
      typeof candidate.recipient_public_key !== 'string' ||
      typeof candidate.origin_peer_id !== 'string' ||
      typeof candidate.acked_at !== 'string' ||
      typeof candidate.reporter_server_public_key !== 'string' ||
      typeof candidate.signature !== 'string' ||
      typeof candidate.hop_count !== 'number' ||
      typeof candidate.max_hops !== 'number'
    ) {
      return null;
    }

    return {
      acked_at: candidate.acked_at,
      event_id: candidate.event_id,
      hop_count: Math.max(0, Math.trunc(candidate.hop_count)),
      max_hops: Math.max(1, Math.trunc(candidate.max_hops)),
      message_id: candidate.message_id,
      origin_peer_id: candidate.origin_peer_id,
      recipient_public_key: candidate.recipient_public_key,
      reporter_server_public_key: normalize_hex(
        candidate.reporter_server_public_key,
      ),
      signature: normalize_hex(candidate.signature),
      type: 'dm.delete.gossip',
    };
  }

  async verify_event_signature(event: DmDeleteGossipEvent) {
    const event_hash = this.create_event_hash(event);

    return ed.verifyAsync(
      ed.etc.hexToBytes(remove_hex_prefix(event.signature)),
      ed.etc.hexToBytes(remove_hex_prefix(event_hash)),
      ed.etc.hexToBytes(remove_hex_prefix(event.reporter_server_public_key)),
    );
  }

  create_forwarded_event(event: DmDeleteGossipEvent) {
    return {
      ...event,
      hop_count: event.hop_count + 1,
    };
  }

  can_forward(event: DmDeleteGossipEvent) {
    return event.hop_count < event.max_hops;
  }

  should_process_event(event_id: string) {
    this.cleanup_dedupe_cache();

    if (this.dedupe_cache.has(event_id)) {
      return false;
    }

    this.dedupe_cache.set(
      event_id,
      Date.now() + this.p2p_config.sync_dedupe_ttl_seconds * 1000,
    );

    return true;
  }

  private cleanup_dedupe_cache() {
    const now_ms = Date.now();

    for (const [event_id, expires_at_ms] of this.dedupe_cache.entries()) {
      if (expires_at_ms <= now_ms) {
        this.dedupe_cache.delete(event_id);
      }
    }
  }

  private create_event_hash(event: Omit<DmDeleteGossipEvent, 'signature'>) {
    const canonical_json = JSON.stringify({
      acked_at: event.acked_at,
      event_id: event.event_id,
      hop_count: event.hop_count,
      max_hops: event.max_hops,
      message_id: event.message_id,
      origin_peer_id: event.origin_peer_id,
      recipient_public_key: event.recipient_public_key,
      reporter_server_public_key: event.reporter_server_public_key,
      type: event.type,
    });
    const hash = createHash('sha256').update(canonical_json).digest('hex');

    return `0x${hash}`;
  }
}
