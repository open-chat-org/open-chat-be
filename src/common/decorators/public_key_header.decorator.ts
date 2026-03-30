import { createParamDecorator, ExecutionContext } from '@nestjs/common';

export const PublicKeyHeader = createParamDecorator(
  (_data: unknown, context: ExecutionContext) => {
    const request = context.switchToHttp().getRequest();

    return request.headers['x-public-key'];
  },
);
