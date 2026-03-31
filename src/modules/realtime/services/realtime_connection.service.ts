import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { get_realtime_config } from '../../../config/realtime.config';
import WebSocket from 'ws';

type ConnectionState = {
  auth_timeout: NodeJS.Timeout | null;
  authenticated: boolean;
  awaiting_pong: boolean;
  missed_pongs: number;
  public_key?: string;
  session_id?: string;
  socket_id: string;
};

@Injectable()
export class RealtimeConnectionService implements OnModuleDestroy {
  private readonly realtime_config = get_realtime_config();
  private readonly client_states = new Map<WebSocket, ConnectionState>();
  private readonly session_clients = new Map<string, Set<WebSocket>>();
  private heartbeat_interval: NodeJS.Timeout | null = null;

  register_client(client: WebSocket, socket_id: string, on_auth_timeout: () => void) {
    const auth_timeout = setTimeout(() => {
      on_auth_timeout();
    }, this.realtime_config.auth_timeout_ms);

    this.client_states.set(client, {
      auth_timeout,
      authenticated: false,
      awaiting_pong: false,
      missed_pongs: 0,
      socket_id,
    });
  }

  mark_authenticated(
    client: WebSocket,
    session_id: string,
    public_key: string,
  ) {
    const existing_state = this.client_states.get(client);

    if (!existing_state) {
      return;
    }

    if (existing_state.auth_timeout) {
      clearTimeout(existing_state.auth_timeout);
    }

    this.client_states.set(client, {
      ...existing_state,
      auth_timeout: null,
      authenticated: true,
      public_key,
      session_id,
    });

    const session_clients = this.session_clients.get(session_id) ?? new Set<WebSocket>();

    session_clients.add(client);
    this.session_clients.set(session_id, session_clients);
  }

  get_authenticated_state(client: WebSocket) {
    const state = this.client_states.get(client);

    if (!state?.authenticated || !state.public_key || !state.session_id) {
      return null;
    }

    return state;
  }

  has_local_session(session_id: string) {
    return (this.session_clients.get(session_id)?.size ?? 0) > 0;
  }

  get_client_by_session_id(session_id: string) {
    const session_clients = this.session_clients.get(session_id);

    if (!session_clients || session_clients.size === 0) {
      return null;
    }

    return [...session_clients][0] ?? null;
  }

  start_heartbeat(on_stale_client: (client: WebSocket) => void) {
    if (this.heartbeat_interval) {
      return;
    }

    this.heartbeat_interval = setInterval(() => {
      for (const [client, state] of this.client_states.entries()) {
        if (!state.authenticated) {
          continue;
        }

        if (state.awaiting_pong) {
          state.missed_pongs += 1;

          if (state.missed_pongs >= this.realtime_config.missed_pong_limit) {
            on_stale_client(client);
            continue;
          }
        }

        state.awaiting_pong = true;

        if (client.readyState === WebSocket.OPEN) {
          client.ping();
        }
      }
    }, this.realtime_config.ping_interval_ms);
  }

  register_pong(client: WebSocket) {
    const state = this.client_states.get(client);

    if (!state) {
      return;
    }

    state.awaiting_pong = false;
    state.missed_pongs = 0;
  }

  send_json(client: WebSocket, payload: unknown) {
    if (client.readyState !== WebSocket.OPEN) {
      return;
    }

    client.send(JSON.stringify(payload));
  }

  close_session_clients(session_id: string, code: number, reason: string) {
    const session_clients = this.session_clients.get(session_id);

    if (!session_clients) {
      return;
    }

    for (const client of session_clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.close(code, reason);
      }
    }
  }

  remove_client(client: WebSocket) {
    const state = this.client_states.get(client);

    if (!state) {
      return null;
    }

    if (state.auth_timeout) {
      clearTimeout(state.auth_timeout);
    }

    if (state.session_id) {
      const session_clients = this.session_clients.get(state.session_id);

      if (session_clients) {
        session_clients.delete(client);

        if (session_clients.size === 0) {
          this.session_clients.delete(state.session_id);
        }
      }
    }

    this.client_states.delete(client);

    return state;
  }

  onModuleDestroy() {
    if (this.heartbeat_interval) {
      clearInterval(this.heartbeat_interval);
    }

    for (const [client, state] of this.client_states.entries()) {
      if (state.auth_timeout) {
        clearTimeout(state.auth_timeout);
      }

      if (client.readyState === WebSocket.OPEN) {
        client.close(1012, 'Server shutting down');
      }
    }
  }
}
