export function create_user_room_name(public_key: string) {
  return `user:${public_key}`;
}

export function create_chat_room_name(room_id: string) {
  return `chat:${room_id}`;
}
