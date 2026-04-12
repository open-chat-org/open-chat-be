# Open Chat Backend

This project uses NestJS for the backend structure and Prisma for database access. The main goal of this README is to keep the codebase consistent as new modules, services, and features are added.

## Project Rules

- Keep the project structure feature-based.
- Use `snake_case` for file names, folder names, and database field names.
- Keep the codebase clean at all times. Remove dead code, avoid duplication, and do not leave temporary or unused structures behind.
- Keep modules small and focused on a single responsibility.
- Do not place business logic inside controllers.
- Keep generated files isolated inside `src/generated` and do not manually edit them.
- Prefer clear, predictable naming over short or clever names.
- Follow the existing NestJS module pattern when adding new features.

## Folder Structure

Use this structure as the default pattern:

```text
src/
  common/           shared helpers, guards, interceptors, constants
  config/           environment and app configuration
  generated/        generated code only, such as Prisma client output
  modules/
    prisma/         prisma module and prisma service
    <feature_name>/ feature-specific module files
  app.module.ts
  main.ts
```

## Feature Module Structure

Each feature should live inside `src/modules/<feature_name>/`.

Recommended structure:

```text
src/modules/chat_room/
  chat_room.module.ts
  chat_room.controller.ts
  chat_room.service.ts
  dto/
    create_chat_room.dto.ts
  entities/
    chat_room.entity.ts
```

Rules:

- One feature folder per domain or business area.
- Group related controller, service, DTO, entity, and repository files together.
- If a feature grows, split it into subfolders like `dto`, `entities`, `interfaces`, and `repositories`.
- Avoid placing unrelated files in the same module.

## Naming Convention

The project naming convention must use `snake_case` wherever possible.

Use `snake_case` for:

- file names
- folder names
- Prisma model fields mapped to database columns
- environment variable keys only when the platform already expects uppercase with underscores, such as `DATABASE_URL`

Examples:

```text
user_profile.service.ts
chat_message.controller.ts
redis.config.ts
public_key
created_at
```

Use these class naming rules:

- Modules: `PascalCase` ending with `Module`
- Services: `PascalCase` ending with `Service`
- Controllers: `PascalCase` ending with `Controller`
- DTOs: `PascalCase` ending with `Dto`
- Entities: `PascalCase` ending with `Entity`

Examples:

```ts
export class ChatRoomModule {}
export class ChatRoomService {}
export class ChatRoomController {}
export class CreateChatRoomDto {}
```

## Code Organization Rules

- Controllers should only handle request and response flow.
- Services should contain business logic.
- Database access should stay inside Prisma usage boundaries or repository-style abstractions if added later.
- Shared utility code should go into `src/common`.
- App and infrastructure configuration should go into `src/config`.
- Avoid duplicated logic across modules. Extract common logic when it is reused.
- Remove unused imports, components, services, and files when they are no longer part of the active implementation.

## Prisma Rules

- Keep Prisma schema changes inside `prisma/schema.prisma`.
- Do not edit files inside `src/generated/prisma` manually.
- Regenerate Prisma client after schema changes.
- Keep database column names in `snake_case`.
- If TypeScript property names need to differ from database names, use Prisma mapping explicitly.

## Config Rules

- Store environment-related logic in `src/config`.
- Keep `.env` keys consistent and documented.
- Validate required environment variables before app startup when possible.
- Never hardcode secrets, URLs, or credentials in source files.

## Realtime Architecture

The backend realtime layer is built on native WebSocket plus Redis.

It is designed as a reusable transport component, not as chat business logic. That means the realtime module is responsible for connection handling, authentication, reconnect behavior, heartbeat, room fanout, and reliable delivery. Future chat features should call the realtime service instead of implementing socket behavior directly.

### Realtime Flow

The realtime connection flow works like this:

1. The client requests `POST /realtime/challenge` with `x-public-key`.
2. The server creates a short-lived challenge, signs it with the server private key, and returns:
   - `challenge_id`
   - `nonce`
   - `expires_at`
   - `server_public_key`
   - `server_signature`
3. The client signs the challenge message with its own private key.
4. The client opens the WebSocket connection and sends the first frame as `auth.connect`.
5. The server verifies:
   - the challenge exists
   - the challenge is not expired
   - the challenge was not already used
   - the client signature matches the provided public key
6. After verification, the server creates or restores the realtime session.
7. The session automatically joins the personal room: `user:{public_key}`.
8. Reliable events are delivered with an envelope that supports ack and retry.
9. Redis is used so the same behavior works across multiple backend nodes.

### Realtime Responsibilities

- WebSocket transport: accept connections and handle realtime frames.
- Signed auth: verify the client owns the public key it claims.
- Session restore: allow reconnect with `last_session_id`.
- Heartbeat: detect stale sockets and close dead connections.
- Rooms: manage `user:{public_key}` and `chat:{room_id}` memberships.
- Reliable delivery: track pending events, accept `delivery.ack`, and retry failed deliveries.
- Redis fanout: publish room events so all backend nodes can deliver to their local clients.

### Realtime Files

Use this module structure as the reference for future realtime work:

```text
src/modules/realtime/
  constants/
    realtime.constants.ts
  services/
    realtime_redis.service.ts
    realtime_challenge.service.ts
    realtime_session.service.ts
    realtime_connection.service.ts
    realtime_room.service.ts
    realtime_delivery.service.ts
    realtime_fanout.service.ts
    realtime.service.ts
  types/
    realtime.types.ts
  utils/
    realtime_keys.util.ts
    realtime_room.util.ts
    realtime_signature.util.ts
  realtime.controller.ts
  realtime.gateway.ts
  realtime.module.ts
```

File responsibilities:

- `realtime.module.ts`: wires the full realtime component together and exports the reusable realtime service.
- `realtime.controller.ts`: exposes REST endpoints related to realtime bootstrap, currently the challenge endpoint.
- `realtime.gateway.ts`: receives WebSocket connections and handles realtime frame events like `auth.connect` and `delivery.ack`.
- `realtime.constants.ts`: shared realtime injection keys and Redis channel names.
- `realtime.types.ts`: shared transport, session, room, and envelope types.
- `realtime_redis.service.ts`: owns Redis clients, subscriptions, and publish/subscribe helpers.
- `realtime_challenge.service.ts`: creates signed auth challenges and verifies challenge usage.
- `realtime_session.service.ts`: creates, restores, touches, and disconnects realtime sessions in Redis.
- `realtime_connection.service.ts`: tracks live WebSocket clients on the current node and manages ping/pong heartbeat.
- `realtime_room.service.ts`: stores and restores room membership and keeps local room indexes in sync.
- `realtime_delivery.service.ts`: stores pending reliable envelopes, accepts ack, and retries unacked events.
- `realtime_fanout.service.ts`: publishes room events through Redis and delivers them to local node sessions.
- `realtime.service.ts`: public internal API for other backend modules to emit to users or rooms and manage room membership.
- `realtime_keys.util.ts`: builds Redis key names for challenges, sessions, rooms, and pending deliveries.
- `realtime_room.util.ts`: creates standardized room names.
- `realtime_signature.util.ts`: normalizes keys and signatures, builds the canonical challenge message, and verifies client signatures.

### Realtime Rules

- Keep transport logic inside the realtime module.
- Do not place chat-specific business rules directly inside the gateway.
- Use `RealtimeService` when another module needs to emit to a user or room.
- Keep Redis key naming centralized in realtime utils.
- Keep all frame payloads typed in `realtime.types.ts`.
- Do not bypass signed challenge auth for client websocket connections.
- Preserve the reliable envelope contract when adding new realtime events.

### Realtime Environment Variables

Document and keep these values consistent:

- `REDIS_URL`
- `WS_PATH`
- `WS_CHALLENGE_TTL_SECONDS`
- `WS_RECONNECT_GRACE_MS`
- `WS_PING_INTERVAL_MS`
- `WS_MISSED_PONG_LIMIT`
- `WS_ACK_TIMEOUT_MS`
- `WS_MAX_RETRIES`

### P2P Keep-Alive Environment Variables

Document and keep these values consistent:

- `P2P_ENABLED`
- `P2P_LISTEN`
- `P2P_BOOTSTRAP`
- `P2P_KEEP_ALIVE_ENABLED`
- `P2P_KEEP_ALIVE_CORE_COUNT`
- `P2P_KEEP_ALIVE_RECONCILE_MS`
- `P2P_KEEP_ALIVE_REDIAL_MS`
- `P2P_RECONNECT_RETRIES`
- `P2P_RECONNECT_RETRY_INTERVAL_MS`
- `P2P_RECONNECT_BACKOFF_FACTOR`
- `P2P_MAX_PARALLEL_RECONNECTS`
- `P2P_DIAL_TIMEOUT_MS`

## Testing Rules

- Place unit tests next to the related source file when practical, using `*.spec.ts`.
- Keep tests focused and readable.
- Write tests for service logic and important module behavior.
- Add tests for bug fixes when the issue can be reproduced in code.

## Do Not

- Do not mix unrelated features in the same folder.
- Do not put complex logic in controllers.
- Do not manually edit generated Prisma files.
- Do not introduce inconsistent naming styles like `camelCase-file.ts` or `PascalCase-folder`.
- Do not create large utility files with unrelated helpers.

## Summary

When adding new code to this project:

- keep folders and files in `snake_case`
- keep classes in `PascalCase`
- keep structure feature-based under `src/modules`
- keep shared logic in `src/common`
- keep config in `src/config`
- keep the codebase clean and remove unused code
- keep generated Prisma code untouched
