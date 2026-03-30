import { ConflictException, Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { UpdateUserProfileDto } from './dto/update_user_profile.dto';

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

  async update_user_profile(
    public_key: string,
    update_user_profile_dto: UpdateUserProfileDto,
  ) {
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
