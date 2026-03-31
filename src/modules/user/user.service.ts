import {
  ConflictException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { UpdateUserProfileDto } from './dto/update_user_profile.dto';
import { verify_profile_signature } from './utils/profile_signature.util';

@Injectable()
export class UserService {
  constructor(private readonly prisma_service: PrismaService) {}

  async store_public_key(public_key: string) {
    return this.prisma_service.user.upsert({
      where: { public_key },
      update: {},
      create: { public_key },
      select: {
        public_key: true,
        createdAt: true,
      },
    });
  }

  async get_user_profile(public_key: string) {
    const user = await this.prisma_service.user.findUnique({
      where: { public_key },
      select: {
        public_key: true,
        username: true,
        name: true,
        quote: true,
        phone: true,
        email: true,
      },
    });

    if (!user) {
      return {
        public_key,
        username: null,
        name: null,
        quote: null,
        phone: null,
        email: null,
      };
    }

    return user;
  }

  async search_users(requester_public_key: string, query: string) {
    const normalized_query = query.trim();
    const matching_users = await this.prisma_service.user.findMany({
      where: {
        public_key: {
          not: requester_public_key,
        },
        OR: [
          {
            username: {
              contains: normalized_query,
              mode: 'insensitive',
            },
          },
          {
            name: {
              contains: normalized_query,
              mode: 'insensitive',
            },
          },
        ],
      },
      select: {
        name: true,
        public_key: true,
        username: true,
      },
      take: 20,
    });

    return matching_users.filter((user) => {
      const normalized_username = user.username?.trim() ?? '';
      const normalized_name = user.name?.trim() ?? '';

      return Boolean(normalized_username || normalized_name);
    });
  }

  async update_user_profile(
    public_key: string,
    update_user_profile_dto: UpdateUserProfileDto,
  ) {
    const is_valid_signature = await verify_profile_signature(
      public_key,
      update_user_profile_dto,
    );

    if (!is_valid_signature) {
      throw new UnauthorizedException('Profile signature verification failed.');
    }

    try {
      return await this.prisma_service.user.upsert({
        where: {
          public_key,
        },
        update: {
          username: this.normalize_optional_field(update_user_profile_dto.username),
          name: this.normalize_optional_field(update_user_profile_dto.name),
          quote: this.normalize_optional_field(update_user_profile_dto.quote),
          phone: this.normalize_optional_field(update_user_profile_dto.phone),
          email: this.normalize_optional_field(update_user_profile_dto.email),
        },
        create: {
          public_key,
          username: this.normalize_optional_field(update_user_profile_dto.username),
          name: this.normalize_optional_field(update_user_profile_dto.name),
          quote: this.normalize_optional_field(update_user_profile_dto.quote),
          phone: this.normalize_optional_field(update_user_profile_dto.phone),
          email: this.normalize_optional_field(update_user_profile_dto.email),
        },
        select: {
          public_key: true,
          username: true,
          name: true,
          quote: true,
          phone: true,
          email: true,
        },
      });
    } catch (error) {
      const error_code =
        typeof error === 'object' && error !== null && 'code' in error
          ? String(error.code)
          : '';

      if (error_code === 'P2002') {
        throw new ConflictException(
          'This username or profile value is already in use.',
        );
      }

      throw error;
    }
  }

  private normalize_optional_field(value?: string) {
    if (value === undefined) {
      return undefined;
    }

    const trimmed_value = value.trim();

    return trimmed_value.length > 0 ? trimmed_value : null;
  }
}
