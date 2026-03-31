import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import { PublicKeyHeader } from '../../common/decorators/public_key_header.decorator';
import { PublicKeyHeaderPipe } from '../../common/pipes/public_key_header.pipe';
import { RegisterX25519PublicKeyDto } from './dto/register_x25519_public_key.dto';
import { SearchUsersDto } from './dto/search_users.dto';
import { UpdateUserProfileDto } from './dto/update_user_profile.dto';
import { UserService } from './user.service';

@Controller('user')
export class UserController {
  constructor(private readonly user_service: UserService) {}

  @Post('public_key')
  @HttpCode(HttpStatus.CREATED)
  async store_public_key(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
  ) {
    return this.user_service.store_public_key(public_key);
  }

  @Post('x25519_public_key')
  @HttpCode(HttpStatus.OK)
  async store_x25519_public_key(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
    @Body() body: RegisterX25519PublicKeyDto,
  ) {
    return this.user_service.store_x25519_public_key(public_key, body);
  }

  @Get('profile')
  async get_user_profile(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
  ) {
    return this.user_service.get_user_profile(public_key);
  }

  @Get('search')
  async search_users(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
    @Query() search_users_dto: SearchUsersDto,
  ) {
    return this.user_service.search_users(public_key, search_users_dto.query);
  }

  @Get('key_bundle')
  async get_user_key_bundle(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) _requester_public_key: string,
    @Query('public_key', new PublicKeyHeaderPipe()) public_key: string,
  ) {
    return this.user_service.get_user_key_bundle(public_key);
  }

  @Patch('profile')
  @HttpCode(HttpStatus.OK)
  async update_user_profile(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
    @Body() body: UpdateUserProfileDto,
  ) {
    return this.user_service.update_user_profile(public_key, body);
  }
}
