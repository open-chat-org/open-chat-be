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
