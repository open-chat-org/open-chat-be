import { BadRequestException, Injectable, PipeTransform } from '@nestjs/common';

@Injectable()
export class PublicKeyHeaderPipe implements PipeTransform {
  transform(value: unknown) {
    if (typeof value !== 'string') {
      throw new BadRequestException('x-public-key header is required.');
    }

    const public_key = value.trim();

    if (!public_key) {
      throw new BadRequestException('x-public-key header is required.');
    }

    if (!/^0x[a-fA-F0-9]{64,130}$/.test(public_key)) {
      throw new BadRequestException(
        'x-public-key header must be a valid hex string starting with 0x and containing 64 to 130 hex characters.',
      );
    }

    return public_key;
  }
}
