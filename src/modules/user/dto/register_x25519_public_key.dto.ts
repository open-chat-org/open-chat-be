import { Transform } from 'class-transformer';
import { IsNotEmpty, IsString, Matches } from 'class-validator';

function normalize_hex_value(value: unknown) {
  if (typeof value !== 'string') {
    return value;
  }

  const trimmed_value = value.trim();

  return trimmed_value.startsWith('0x') ? trimmed_value : `0x${trimmed_value}`;
}

export class RegisterX25519PublicKeyDto {
  @Transform(({ value }) => normalize_hex_value(value))
  @IsString()
  @IsNotEmpty()
  @Matches(/^0x[a-fA-F0-9]{64}$/, {
    message:
      'x25519_public_key must be a valid hex string starting with 0x and containing 64 hex characters.',
  })
  x25519_public_key: string;

  @Transform(({ value }) => normalize_hex_value(value))
  @IsString()
  @IsNotEmpty()
  @Matches(/^0x[a-fA-F0-9]{64}$/, {
    message:
      'x25519_public_key_hash must be a valid hex string starting with 0x and containing 64 hex characters.',
  })
  x25519_public_key_hash: string;

  @Transform(({ value }) => normalize_hex_value(value))
  @IsString()
  @IsNotEmpty()
  @Matches(/^0x[a-fA-F0-9]{128}$/, {
    message:
      'x25519_public_key_signature must be a valid hex string starting with 0x and containing 128 hex characters.',
  })
  x25519_public_key_signature: string;
}
