import {
  get_optional_env_string,
  get_required_env_string,
} from './env.config';

export type RedisConfig = {
  is_required: boolean;
  url: string | null;
};

export function get_redis_config(): RedisConfig {
  const environment = get_optional_env_string('NODE_ENV', 'development');
  const redis_url =
    environment === 'test'
      ? (get_optional_env_string('REDIS_URL') ?? null)
      : get_required_env_string('REDIS_URL');

  return {
    is_required: environment !== 'test',
    url: redis_url,
  };
}
