export function create_auth_challenge_key(challenge_id: string) {
  return `realtime:challenge:${challenge_id}`;
}

export function create_session_key(session_id: string) {
  return `realtime:session:${session_id}`;
}

export function create_session_rooms_key(session_id: string) {
  return `realtime:session:${session_id}:rooms`;
}

export function create_session_pending_key(session_id: string) {
  return `realtime:session:${session_id}:pending`;
}

export function create_room_fanout_channel(room_name: string) {
  return `realtime:fanout:room:${room_name}`;
}

export function create_user_sessions_key(public_key: string) {
  return `realtime:user:${public_key}:sessions`;
}
