import {isIP} from 'node:net'
import process from 'node:process'
import {checkServerIdentity, type ConnectionOptions} from 'node:tls'

function readHostname(
  value: string,
  setting: 'REDIS_HOST_OVERRIDE' | 'REDIS_TLS_IDENTITY',
): string {
  const unwrapped =
    value.startsWith('[') && value.endsWith(']') ? value.slice(1, -1) : value

  if (isIP(unwrapped) === 6) {
    return `[${unwrapped}]`
  }

  const isDnsHostname =
    value.length <= 253 &&
    value
      .split('.')
      .every((label) => /^[a-z\d](?:[a-z\d\-]{0,61}[a-z\d])?$/iv.test(label))

  if (isIP(value) === 4 || isDnsHostname) {
    return value
  }

  throw new Error(`${setting} must contain only a hostname or IP address`)
}

export function readRedisUrl(environment = process.env): string {
  const original = environment.REDIS_URL ?? 'redis://localhost:6380'
  const override = environment.REDIS_HOST_OVERRIDE?.trim()

  if (!override) {
    return original
  }

  const hostname = readHostname(override, 'REDIS_HOST_OVERRIDE')
  let url: URL
  try {
    url = new URL(original)
  } catch {
    throw new Error(
      'REDIS_URL must be a valid Redis URL when overriding its host',
    )
  }

  if (!['redis:', 'rediss:'].includes(url.protocol) || !url.hostname) {
    throw new Error(
      'REDIS_URL must be a valid Redis URL when overriding its host',
    )
  }

  url.hostname = hostname
  if (url.hostname !== new URL(`redis://${hostname}`).hostname) {
    throw new Error('REDIS_HOST_OVERRIDE could not be applied')
  }

  return url.href
}

export function getRedisTlsOptions(
  ca: ConnectionOptions['ca'],
  environment = process.env,
): ConnectionOptions | undefined {
  const configuredIdentity = environment.REDIS_TLS_IDENTITY?.trim()
  if (!configuredIdentity) {
    return ca === undefined ? undefined : {ca}
  }

  const identity = readHostname(configuredIdentity, 'REDIS_TLS_IDENTITY')
  const hostname = identity.startsWith('[') ? identity.slice(1, -1) : identity
  let redisUrl: URL
  try {
    redisUrl = new URL(environment.REDIS_URL ?? '')
  } catch {
    throw new Error(
      'REDIS_TLS_IDENTITY requires a rediss URL and a non-empty CA',
    )
  }

  if (
    redisUrl.protocol !== 'rediss:' ||
    !redisUrl.hostname ||
    ca === undefined ||
    Array.isArray(ca) ||
    (typeof ca === 'string' ? ca.trim().length === 0 : ca.length === 0)
  ) {
    throw new Error(
      'REDIS_TLS_IDENTITY requires a rediss URL and a non-empty CA',
    )
  }

  return {
    ca,
    rejectUnauthorized: true,
    checkServerIdentity: (_hostname, certificate) =>
      checkServerIdentity(hostname, certificate),
  }
}
