import {
  IsEmail,
  IsOptional,
  IsString,
  MaxLength,
} from 'class-validator';

export class UpdateUserProfileDto {
  @IsOptional()
  @IsString()
  @MaxLength(40)
  username?: string;

  @IsOptional()
  @IsString()
  @MaxLength(80)
  name?: string;

  @IsOptional()
  @IsString()
  @MaxLength(200)
  quote?: string;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  phone?: string;

  @IsOptional()
  @IsEmail()
  @MaxLength(120)
  email?: string;
}
