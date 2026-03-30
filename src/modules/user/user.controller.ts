import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Patch,
  Post,
} from '@nestjs/common';
import { PublicKeyHeader } from '../../common/decorators/public_key_header.decorator';
import { PublicKeyHeaderPipe } from '../../common/pipes/public_key_header.pipe';
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

  @Get('profile')
  async get_user_profile(
    @PublicKeyHeader(new PublicKeyHeaderPipe()) public_key: string,
  ) {
    return this.user_service.get_user_profile(public_key);
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
