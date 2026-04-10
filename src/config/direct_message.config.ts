export function get_direct_message_config() {
  return {
    algorithm: 'x25519-xchacha20poly1305',
    cleanup_interval_ms: 60 * 60 * 1000,
    retention_days: 90,
  };
}
