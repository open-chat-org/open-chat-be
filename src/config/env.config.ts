export function get_optional_env_string(
  key: string,
  default_value?: string,
): string | undefined {
  const raw_value = process.env[key];

  if (raw_value === undefined) {
    return default_value;
  }

  const trimmed_value = raw_value.trim();

  if (!trimmed_value) {
    return default_value;
  }

  return trimmed_value;
}

export function get_required_env_string(key: string): string {
  const value = get_optional_env_string(key);

  if (!value) {
    throw new Error(`${key} environment variable is required.`);
  }

  return value;
}

export function get_optional_env_number(
  key: string,
  default_value: number,
): number {
  const raw_value = get_optional_env_string(key);

  if (!raw_value) {
    return default_value;
  }

  const parsed_value = Number(raw_value);

  if (Number.isNaN(parsed_value)) {
    throw new Error(`${key} environment variable must be a valid number.`);
  }

  return parsed_value;
}
