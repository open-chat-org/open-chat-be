import { Injectable, OnModuleInit } from '@nestjs/common';
import { createPrivateKey, generateKeyPairSync, sign } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';

type ServerPublicKeyFile = {
  algorithm: 'ed25519';
  created_at: string;
  public_key: string;
};

type ServerPrivateKeyJwk = {
  crv: 'Ed25519';
  d: string;
  key_ops?: string[];
  kty: 'OKP';
  x: string;
};

type ServerPrivateKeyFile = {
  algorithm: 'ed25519';
  created_at: string;
  private_key_jwk: ServerPrivateKeyJwk;
  public_key: string;
};

type ServerIdentity = {
  private_key_jwk: ServerPrivateKeyJwk;
  public_key: string;
};

@Injectable()
export class ServerIdentityService implements OnModuleInit {
  private readonly storage_directory = path.join(
    process.cwd(),
    'storage',
    'server_identity',
  );

  private readonly public_key_path = path.join(
    this.storage_directory,
    'server_public_key.json',
  );

  private readonly private_key_path = path.join(
    this.storage_directory,
    'server_private_key.json',
  );

  private server_identity: ServerIdentity | null = null;

  async onModuleInit(): Promise<void> {
    await this.ensure_server_identity();
  }

  async ensure_server_identity(): Promise<ServerIdentity> {
    if (this.server_identity) {
      return this.server_identity;
    }

    await fs.mkdir(this.storage_directory, { recursive: true });

    const existing_server_identity = await this.read_existing_server_identity();

    if (existing_server_identity) {
      this.server_identity = existing_server_identity;
      return existing_server_identity;
    }

    const generated_server_identity = await this.generate_and_store_server_identity();
    this.server_identity = generated_server_identity;

    return generated_server_identity;
  }

  async get_public_key() {
    const server_identity = await this.ensure_server_identity();

    return {
      algorithm: 'ed25519',
      public_key: server_identity.public_key,
    };
  }

  async sign_message(message: string | Uint8Array) {
    const server_identity = await this.ensure_server_identity();
    const private_key = createPrivateKey({
      key: server_identity.private_key_jwk,
      format: 'jwk',
    });
    const signature = sign(
      null,
      typeof message === 'string' ? Buffer.from(message, 'utf8') : message,
      private_key,
    );

    return `0x${signature.toString('hex')}`;
  }

  private async read_existing_server_identity(): Promise<ServerIdentity | null> {
    try {
      const [public_key_file_content, private_key_file_content] = await Promise.all([
        fs.readFile(this.public_key_path, 'utf8'),
        fs.readFile(this.private_key_path, 'utf8'),
      ]);

      const public_key_file = JSON.parse(
        public_key_file_content,
      ) as ServerPublicKeyFile;
      const private_key_file = JSON.parse(
        private_key_file_content,
      ) as ServerPrivateKeyFile;

      if (
        public_key_file.algorithm !== 'ed25519' ||
        private_key_file.algorithm !== 'ed25519' ||
        !public_key_file.public_key ||
        !private_key_file.public_key ||
        !private_key_file.private_key_jwk?.d ||
        !private_key_file.private_key_jwk?.x
      ) {
        return null;
      }

      if (public_key_file.public_key !== private_key_file.public_key) {
        return null;
      }

      return {
        private_key_jwk: private_key_file.private_key_jwk,
        public_key: public_key_file.public_key,
      };
    } catch {
      return null;
    }
  }

  private async generate_and_store_server_identity(): Promise<ServerIdentity> {
    const { privateKey, publicKey } = generateKeyPairSync('ed25519');
    const public_key_jwk = publicKey.export({ format: 'jwk' }) as {
      crv: 'Ed25519';
      kty: 'OKP';
      x: string;
    };
    const private_key_jwk = privateKey.export({
      format: 'jwk',
    }) as ServerPrivateKeyJwk;

    const public_key = this.base64_url_to_hex(public_key_jwk.x);
    const created_at = new Date().toISOString();

    const public_key_file: ServerPublicKeyFile = {
      algorithm: 'ed25519',
      created_at,
      public_key,
    };

    const private_key_file: ServerPrivateKeyFile = {
      algorithm: 'ed25519',
      created_at,
      private_key_jwk,
      public_key,
    };

    await Promise.all([
      fs.writeFile(this.public_key_path, JSON.stringify(public_key_file, null, 2)),
      fs.writeFile(
        this.private_key_path,
        JSON.stringify(private_key_file, null, 2),
      ),
    ]);

    return {
      private_key_jwk,
      public_key,
    };
  }

  private base64_url_to_hex(value: string) {
    const base64_value = value.replace(/-/g, '+').replace(/_/g, '/');
    const normalized_base64_value =
      base64_value + '='.repeat((4 - (base64_value.length % 4)) % 4);

    return `0x${Buffer.from(normalized_base64_value, 'base64').toString('hex')}`;
  }
}
