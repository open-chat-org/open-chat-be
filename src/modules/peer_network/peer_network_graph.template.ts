export function create_p2p_graph_html() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Open Chat P2P Graph</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1320;
      --surface: #121d2e;
      --surface-muted: #18253a;
      --line: #27364f;
      --text: #e6edf7;
      --text-muted: #9fb0c9;
      --accent: #23c18f;
      --warn: #f7b955;
      --error: #f26b7a;
      --info: #73b2ff;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      background: radial-gradient(circle at top right, #1a2843 0%, var(--bg) 60%);
      color: var(--text);
      font-family: "SF Pro Text", "Segoe UI", sans-serif;
      min-height: 100vh;
      padding: 18px;
    }

    .layout {
      display: grid;
      gap: 14px;
      grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr);
      min-height: calc(100vh - 36px);
    }

    .panel {
      background: linear-gradient(180deg, var(--surface) 0%, #0f1a2b 100%);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 12px 35px rgba(0, 0, 0, 0.28);
      overflow: hidden;
    }

    .panel-header {
      align-items: center;
      border-bottom: 1px solid var(--line);
      display: flex;
      gap: 12px;
      justify-content: space-between;
      padding: 12px 14px;
    }

    .panel-title {
      font-size: 14px;
      font-weight: 700;
      letter-spacing: 0.3px;
      margin: 0;
    }

    .panel-subtitle {
      color: var(--text-muted);
      font-size: 12px;
      margin-top: 2px;
    }

    .status {
      color: var(--text-muted);
      font-size: 12px;
      font-weight: 600;
      white-space: nowrap;
    }

    .status-dot {
      background: var(--warn);
      border-radius: 50%;
      display: inline-block;
      height: 8px;
      margin-right: 6px;
      width: 8px;
    }

    .status-dot.online {
      background: var(--accent);
      box-shadow: 0 0 8px rgba(35, 193, 143, 0.7);
    }

    .status-dot.error {
      background: var(--error);
      box-shadow: 0 0 8px rgba(242, 107, 122, 0.6);
    }

    .graph-shell {
      display: grid;
      grid-template-rows: auto 1fr;
      min-height: 0;
    }

    .graph-meta {
      border-bottom: 1px solid var(--line);
      color: var(--text-muted);
      display: flex;
      flex-wrap: wrap;
      font-size: 12px;
      gap: 10px 16px;
      padding: 10px 14px;
    }

    .graph-meta strong {
      color: var(--text);
      font-weight: 700;
    }

    #topology_svg {
      display: block;
      height: 100%;
      min-height: 350px;
      width: 100%;
    }

    .right-side {
      display: grid;
      gap: 14px;
      grid-template-rows: minmax(0, 1fr) minmax(0, 1fr);
      min-height: 0;
    }

    .filters {
      align-items: center;
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      padding: 10px 14px;
    }

    .filters input {
      background: var(--surface-muted);
      border: 1px solid var(--line);
      border-radius: 9px;
      color: var(--text);
      font-size: 12px;
      min-width: 0;
      padding: 9px 10px;
      width: 100%;
    }

    .filters input:focus {
      border-color: #4a6fa5;
      outline: none;
    }

    .timeline {
      list-style: none;
      margin: 0;
      max-height: calc(100% - 56px);
      overflow: auto;
      padding: 10px;
    }

    .timeline-item {
      background: rgba(24, 37, 58, 0.65);
      border: 1px solid var(--line);
      border-radius: 10px;
      cursor: pointer;
      margin-bottom: 8px;
      padding: 9px 10px;
      transition: border-color 140ms ease, transform 140ms ease;
    }

    .timeline-item:hover {
      border-color: #4b6087;
      transform: translateY(-1px);
    }

    .timeline-row-top {
      align-items: center;
      display: flex;
      gap: 8px;
      justify-content: space-between;
    }

    .timeline-event-name {
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
    }

    .timeline-time {
      color: var(--text-muted);
      font-size: 11px;
    }

    .timeline-meta {
      color: var(--text-muted);
      font-size: 11px;
      margin-top: 5px;
      word-break: break-word;
    }

    .severity-badge {
      border: 1px solid var(--line);
      border-radius: 999px;
      display: inline-flex;
      font-size: 10px;
      font-weight: 700;
      line-height: 1;
      padding: 4px 7px;
      text-transform: uppercase;
    }

    .severity-info {
      color: var(--info);
    }

    .severity-warn {
      color: var(--warn);
    }

    .severity-error {
      color: var(--error);
    }

    .flow-content {
      max-height: calc(100% - 48px);
      overflow: auto;
      padding: 12px 14px;
    }

    .flow-input {
      background: var(--surface-muted);
      border: 1px solid var(--line);
      border-radius: 9px;
      color: var(--text);
      font-size: 12px;
      margin: 8px 14px 0;
      padding: 9px 10px;
      width: calc(100% - 28px);
    }

    .flow-stage {
      border-left: 2px solid #2d4d78;
      margin-bottom: 10px;
      padding-left: 10px;
    }

    .flow-stage-name {
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
    }

    .flow-stage-meta {
      color: var(--text-muted);
      font-size: 11px;
      margin-top: 2px;
    }

    .empty {
      color: var(--text-muted);
      font-size: 12px;
      padding: 20px;
      text-align: center;
    }

    @media (max-width: 1180px) {
      .layout {
        grid-template-columns: 1fr;
        grid-template-rows: minmax(0, 1fr) minmax(0, 1fr);
      }
    }
  </style>
</head>
<body>
  <main class="layout">
    <section class="panel graph-shell">
      <header class="panel-header">
        <div>
          <h1 class="panel-title">P2P Network Graph</h1>
          <div class="panel-subtitle">Per-server topology + live peer connectivity.</div>
        </div>
        <div class="status" id="stream_status">
          <span class="status-dot" id="stream_status_dot"></span>
          <span id="stream_status_text">Connecting stream...</span>
        </div>
      </header>
      <div class="graph-meta">
        <div><strong>Local Peer:</strong> <span id="local_peer_id">-</span></div>
        <div><strong>Connected Peers:</strong> <span id="connected_peer_count">0</span></div>
        <div><strong>Updated:</strong> <span id="topology_updated_at">-</span></div>
      </div>
      <svg id="topology_svg" viewBox="0 0 900 520" preserveAspectRatio="xMidYMid meet"></svg>
    </section>

    <section class="right-side">
      <article class="panel">
        <header class="panel-header">
          <div>
            <h2 class="panel-title">Event Timeline</h2>
            <div class="panel-subtitle">Newest first. Click an event to inspect its message flow.</div>
          </div>
        </header>
        <div class="filters">
          <input id="filter_peer_id" placeholder="Filter by peer id" />
          <input id="filter_message_id" placeholder="Filter by message id" />
          <input id="filter_event_type" placeholder="Filter by event type" />
        </div>
        <ul class="timeline" id="timeline_list"></ul>
      </article>

      <article class="panel">
        <header class="panel-header">
          <div>
            <h2 class="panel-title">Message Flow</h2>
            <div class="panel-subtitle">Lifecycle view for one message id.</div>
          </div>
        </header>
        <input id="flow_message_id" class="flow-input" placeholder="Paste/select message id to inspect flow" />
        <div class="flow-content" id="flow_content"></div>
      </article>
    </section>
  </main>

  <script>
    (function () {
      var state = {
        events: [],
        topology: null
      };

      var max_events_in_memory = 2000;
      var stream_reconnect_ms = 1500;
      var topology_refresh_ms = 5000;
      var stream_retry_timeout = null;

      function by_id(id) {
        return document.getElementById(id);
      }

      function to_local_time(iso) {
        if (!iso) {
          return '-';
        }

        var date = new Date(iso);
        if (Number.isNaN(date.getTime())) {
          return String(iso);
        }

        return date.toLocaleTimeString();
      }

      function set_stream_status(mode, text) {
        var status_text = by_id('stream_status_text');
        var status_dot = by_id('stream_status_dot');

        status_text.textContent = text;
        status_dot.classList.remove('online');
        status_dot.classList.remove('error');

        if (mode === 'online') {
          status_dot.classList.add('online');
        } else if (mode === 'error') {
          status_dot.classList.add('error');
        }
      }

      async function fetch_json(url) {
        var response = await fetch(url);
        if (!response.ok) {
          throw new Error('Request failed: ' + response.status + ' ' + response.statusText);
        }
        return response.json();
      }

      function short_peer(peer_id) {
        if (!peer_id) {
          return '-';
        }
        return peer_id.length > 18 ? peer_id.slice(0, 10) + '...' + peer_id.slice(-6) : peer_id;
      }

      function render_topology() {
        var topology = state.topology;
        var svg = by_id('topology_svg');
        svg.innerHTML = '';

        if (!topology || !topology.enabled || !topology.local_node.peer_id) {
          var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          text.setAttribute('x', '50%');
          text.setAttribute('y', '50%');
          text.setAttribute('text-anchor', 'middle');
          text.setAttribute('fill', '#8fa2be');
          text.setAttribute('font-size', '14');
          text.textContent = 'P2P is disabled or node not started on this server.';
          svg.appendChild(text);

          by_id('local_peer_id').textContent = '-';
          by_id('connected_peer_count').textContent = '0';
          by_id('topology_updated_at').textContent = to_local_time(topology ? topology.generated_at : null);
          return;
        }

        var local_peer_id = topology.local_node.peer_id;
        var connected_peers = topology.connected_peers || [];

        by_id('local_peer_id').textContent = local_peer_id;
        by_id('connected_peer_count').textContent = String(connected_peers.length);
        by_id('topology_updated_at').textContent = to_local_time(topology.generated_at);

        var width = 900;
        var height = 520;
        var center_x = width / 2;
        var center_y = height / 2;
        var radius = Math.max(120, Math.min(width, height) * 0.33);

        var local_pos = { x: center_x, y: center_y };
        var peer_positions = [];

        for (var i = 0; i < connected_peers.length; i += 1) {
          var angle = connected_peers.length === 1
            ? 0
            : (i / connected_peers.length) * (Math.PI * 2);
          peer_positions.push({
            peer: connected_peers[i],
            x: center_x + Math.cos(angle) * radius,
            y: center_y + Math.sin(angle) * radius
          });
        }

        for (var line_index = 0; line_index < peer_positions.length; line_index += 1) {
          var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
          line.setAttribute('x1', String(local_pos.x));
          line.setAttribute('y1', String(local_pos.y));
          line.setAttribute('x2', String(peer_positions[line_index].x));
          line.setAttribute('y2', String(peer_positions[line_index].y));
          line.setAttribute('stroke', '#35517b');
          line.setAttribute('stroke-width', '1.8');
          line.setAttribute('opacity', '0.9');
          svg.appendChild(line);
        }

        render_node(svg, local_pos.x, local_pos.y, short_peer(local_peer_id), '#23c18f', local_peer_id, true);

        for (var node_index = 0; node_index < peer_positions.length; node_index += 1) {
          var peer_position = peer_positions[node_index];
          render_node(
            svg,
            peer_position.x,
            peer_position.y,
            short_peer(peer_position.peer.peer_id),
            '#73b2ff',
            peer_position.peer.peer_id,
            false
          );
        }
      }

      function render_node(svg, x, y, label, color, peer_id, is_local) {
        var group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        group.style.cursor = 'pointer';
        group.addEventListener('click', function () {
          by_id('filter_peer_id').value = peer_id || '';
          render_timeline();
        });

        var circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', String(x));
        circle.setAttribute('cy', String(y));
        circle.setAttribute('r', is_local ? '30' : '24');
        circle.setAttribute('fill', color);
        circle.setAttribute('opacity', is_local ? '0.95' : '0.88');
        circle.setAttribute('stroke', '#0f1a2b');
        circle.setAttribute('stroke-width', '2');

        var text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', String(x));
        text.setAttribute('y', String(y + (is_local ? 52 : 42)));
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('fill', '#dce6f4');
        text.setAttribute('font-size', is_local ? '12' : '11');
        text.setAttribute('font-weight', is_local ? '700' : '600');
        text.textContent = label;

        group.appendChild(circle);
        group.appendChild(text);
        svg.appendChild(group);
      }

      function get_filtered_events() {
        var peer_filter = by_id('filter_peer_id').value.trim();
        var message_filter = by_id('filter_message_id').value.trim();
        var event_filter = by_id('filter_event_type').value.trim().toLowerCase();

        return state.events.filter(function (event) {
          if (peer_filter && event.peer_id !== peer_filter) {
            return false;
          }

          if (message_filter && event.message_id !== message_filter) {
            return false;
          }

          if (event_filter && String(event.event_type || '').toLowerCase().indexOf(event_filter) === -1) {
            return false;
          }

          return true;
        });
      }

      function render_timeline() {
        var timeline_list = by_id('timeline_list');
        var events = get_filtered_events();
        timeline_list.innerHTML = '';

        if (events.length === 0) {
          var empty = document.createElement('li');
          empty.className = 'empty';
          empty.textContent = 'No trace events match current filters yet.';
          timeline_list.appendChild(empty);
          return;
        }

        for (var index = 0; index < events.length; index += 1) {
          var event = events[index];
          var item = document.createElement('li');
          item.className = 'timeline-item';
          item.dataset.messageId = event.message_id || '';
          item.addEventListener('click', (function (selected_event) {
            return function () {
              if (selected_event.message_id) {
                by_id('flow_message_id').value = selected_event.message_id;
              }
              render_message_flow();
            };
          })(event));

          var top_row = document.createElement('div');
          top_row.className = 'timeline-row-top';

          var left = document.createElement('span');
          left.className = 'timeline-event-name';
          left.textContent = event.event_type;

          var right = document.createElement('div');
          right.style.display = 'flex';
          right.style.alignItems = 'center';
          right.style.gap = '8px';

          var badge = document.createElement('span');
          badge.className = 'severity-badge severity-' + String(event.severity || 'info');
          badge.textContent = String(event.severity || 'info');

          var time = document.createElement('span');
          time.className = 'timeline-time';
          time.textContent = to_local_time(event.timestamp);

          right.appendChild(badge);
          right.appendChild(time);
          top_row.appendChild(left);
          top_row.appendChild(right);

          var meta = document.createElement('div');
          meta.className = 'timeline-meta';
          meta.textContent =
            'source=' + (event.source || '-') +
            (event.peer_id ? ' | peer=' + short_peer(event.peer_id) : '') +
            (event.message_id ? ' | message=' + event.message_id : '') +
            (event.session_id ? ' | session=' + short_peer(event.session_id) : '');

          item.appendChild(top_row);
          item.appendChild(meta);
          timeline_list.appendChild(item);
        }
      }

      function render_message_flow() {
        var flow_message_id = by_id('flow_message_id').value.trim();
        var flow_content = by_id('flow_content');
        flow_content.innerHTML = '';

        if (!flow_message_id) {
          var empty = document.createElement('div');
          empty.className = 'empty';
          empty.textContent = 'Select an event with a message_id or paste one above.';
          flow_content.appendChild(empty);
          return;
        }

        var flow_events = state.events
          .filter(function (event) {
            return event.message_id === flow_message_id;
          })
          .sort(function (a, b) {
            return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
          });

        if (flow_events.length === 0) {
          var no_data = document.createElement('div');
          no_data.className = 'empty';
          no_data.textContent = 'No events recorded for message_id: ' + flow_message_id;
          flow_content.appendChild(no_data);
          return;
        }

        for (var index = 0; index < flow_events.length; index += 1) {
          var event = flow_events[index];
          var previous = index > 0 ? flow_events[index - 1] : null;
          var delta_ms = previous
            ? Math.max(0, new Date(event.timestamp).getTime() - new Date(previous.timestamp).getTime())
            : null;

          var stage = document.createElement('div');
          stage.className = 'flow-stage';

          var name = document.createElement('div');
          name.className = 'flow-stage-name';
          name.textContent = event.event_type;

          var meta = document.createElement('div');
          meta.className = 'flow-stage-meta';
          meta.textContent =
            to_local_time(event.timestamp) +
            ' | source=' + event.source +
            (delta_ms == null ? '' : ' | +' + delta_ms + 'ms from previous');

          stage.appendChild(name);
          stage.appendChild(meta);
          flow_content.appendChild(stage);
        }
      }

      function push_event(event) {
        state.events.unshift(event);
        if (state.events.length > max_events_in_memory) {
          state.events = state.events.slice(0, max_events_in_memory);
        }
      }

      async function refresh_topology() {
        try {
          state.topology = await fetch_json('/p2p/topology');
          render_topology();
        } catch (error) {
          set_stream_status('error', 'Topology fetch failed');
          console.error(error);
        }
      }

      async function load_initial_trace() {
        try {
          state.events = await fetch_json('/p2p/trace?limit=300');
          render_timeline();
          render_message_flow();
        } catch (error) {
          console.error(error);
        }
      }

      function subscribe_trace_stream() {
        set_stream_status('info', 'Connecting stream...');
        var event_source = new EventSource('/p2p/trace/stream');

        event_source.addEventListener('ready', function () {
          set_stream_status('online', 'Live stream connected');
        });

        event_source.addEventListener('trace', function (message_event) {
          try {
            var trace_event = JSON.parse(message_event.data);
            push_event(trace_event);
            render_timeline();
            render_message_flow();

            if (trace_event.source === 'p2p') {
              void refresh_topology();
            }
          } catch (error) {
            console.error(error);
          }
        });

        event_source.addEventListener('heartbeat', function () {
          set_stream_status('online', 'Live stream connected');
        });

        event_source.onerror = function () {
          set_stream_status('error', 'Stream dropped. Reconnecting...');
          event_source.close();
          if (stream_retry_timeout) {
            clearTimeout(stream_retry_timeout);
          }
          stream_retry_timeout = setTimeout(subscribe_trace_stream, stream_reconnect_ms);
        };
      }

      function attach_filter_handlers() {
        by_id('filter_peer_id').addEventListener('input', render_timeline);
        by_id('filter_message_id').addEventListener('input', function () {
          render_timeline();
          render_message_flow();
        });
        by_id('filter_event_type').addEventListener('input', render_timeline);
        by_id('flow_message_id').addEventListener('input', render_message_flow);
      }

      async function boot() {
        attach_filter_handlers();
        await refresh_topology();
        await load_initial_trace();
        subscribe_trace_stream();
        setInterval(refresh_topology, topology_refresh_ms);
      }

      void boot();
    })();
  </script>
</body>
</html>`;
}

