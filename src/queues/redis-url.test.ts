import assert from 'node:assert/strict'
import {Buffer} from 'node:buffer'
import test from 'node:test'
import {checkServerIdentity, type PeerCertificate} from 'node:tls'
import {Redis} from 'ioredis'
import {getRedisTlsOptions, readRedisUrl} from './redis-url.js'

const ORIGINAL_URL =
  'rediss://test-user:p%40ss%3Aword@203.0.113.10:6379/2?family=4&connectionName=offline-test'

function certificateWithNames(subjectaltname: string): PeerCertificate {
  return {
    ca: false,
    raw: Buffer.alloc(0),
    subject: {},
    issuer: {},
    valid_from: '',
    valid_to: '',
    serialNumber: '',
    fingerprint: '',
    fingerprint256: '',
    fingerprint512: '',
    subjectaltname,
  }
}

void test('Redis host override is optional and preserves the original string exactly', () => {
  assert.equal(readRedisUrl({}), 'redis://localhost:6380')
  for (const override of [undefined, '', '   ']) {
    assert.equal(
      readRedisUrl({REDIS_URL: ORIGINAL_URL, REDIS_HOST_OVERRIDE: override}),
      ORIGINAL_URL,
    )
  }

  assert.equal(readRedisUrl({REDIS_URL: ''}), '')
  assert.equal(readRedisUrl({REDIS_URL: '/tmp/redis.sock'}), '/tmp/redis.sock')
})

void test('Redis host override accepts only a hostname or IP while preserving connection parameters', () => {
  const original = new URL(ORIGINAL_URL)
  for (const [override, expected] of [
    ['172.16.0.10', '172.16.0.10'],
    [' redis.internal.test ', 'redis.internal.test'],
    ['localhost', 'localhost'],
    ['fd00::10', '[fd00::10]'],
    ['[fd00::10]', '[fd00::10]'],
    ['FD00:0000:0000:0000:0000:0000:0000:0010', '[fd00::10]'],
  ]) {
    const result = new URL(
      readRedisUrl({REDIS_URL: ORIGINAL_URL, REDIS_HOST_OVERRIDE: override}),
    )
    assert.equal(result.hostname, expected)
    for (const key of [
      'protocol',
      'username',
      'password',
      'port',
      'pathname',
      'search',
    ] as const) {
      assert.equal(result[key], original[key])
    }
  }
})

void test('Redis host override rejects injected URL components and invalid hostname labels', () => {
  for (const override of [
    'redis://host',
    'host:6379',
    'user@host',
    'host/path',
    'host?db=0',
    'host#fragment',
    'host%2fpath',
    'host name',
    'host\nname',
    'host_name',
    'host..test',
    '-host.test',
    'host-.test',
    `${'a'.repeat(64)}.test`,
    Array.from({length: 5}, () => 'a'.repeat(60)).join('.'),
    '[127.0.0.1]',
    '[fd00::10]:6379',
  ]) {
    assert.throws(
      () =>
        readRedisUrl({REDIS_URL: ORIGINAL_URL, REDIS_HOST_OVERRIDE: override}),
      /^Error: REDIS_HOST_OVERRIDE must contain only a hostname or IP address$/v,
    )
  }
})

void test('invalid source URLs fail without exposing credentials or URL details', () => {
  for (const url of [
    'not-a-url:private-credential',
    'https://test-user:private-credential@example.invalid',
    'redis:///missing-host',
  ]) {
    assert.throws(
      () => readRedisUrl({REDIS_URL: url, REDIS_HOST_OVERRIDE: '172.16.0.10'}),
      /^Error: REDIS_URL must be a valid Redis URL when overriding its host$/v,
    )
  }
})

void test('IORedis receives the private host and keeps credentials, port, database and TLS', () => {
  const previous = new Redis(ORIGINAL_URL, {lazyConnect: true})
  const effective = new Redis(
    readRedisUrl({REDIS_URL: ORIGINAL_URL, REDIS_HOST_OVERRIDE: '172.16.0.10'}),
    {lazyConnect: true},
  )
  try {
    assert.equal(effective.options.host, '172.16.0.10')
    for (const key of [
      'username',
      'password',
      'port',
      'db',
      'family',
      'connectionName',
      'tls',
    ] as const) {
      assert.deepEqual(effective.options[key], previous.options[key])
    }
  } finally {
    previous.disconnect(false)
    effective.disconnect(false)
  }
})

void test('TLS identity validation matches the new private IP and rejects the old endpoint', () => {
  const {hostname} = new URL(
    readRedisUrl({REDIS_URL: ORIGINAL_URL, REDIS_HOST_OVERRIDE: '172.16.0.10'}),
  )
  const certificate = certificateWithNames('IP Address:172.16.0.10')
  assert.equal(checkServerIdentity(hostname, certificate), undefined)
  assert.ok(
    checkServerIdentity(hostname, {
      ...certificate,
      subjectaltname: 'IP Address:203.0.113.10',
    }) instanceof Error,
  )
})

void test('TLS configuration without an explicit identity retains historical behavior', () => {
  assert.equal(getRedisTlsOptions(undefined, {}), undefined)
  assert.deepEqual(getRedisTlsOptions('', {}), {ca: ''})
  assert.deepEqual(
    getRedisTlsOptions('test-ca', {REDIS_URL: 'redis://localhost'}),
    {
      ca: 'test-ca',
    },
  )
})

void test('explicit TLS identity validates the certificate independently from the private transport address', () => {
  const options = getRedisTlsOptions('test-ca', {
    REDIS_URL: ORIGINAL_URL,
    REDIS_HOST_OVERRIDE: '172.16.0.10',
    REDIS_TLS_IDENTITY: '203.0.113.10',
  })
  assert.equal(options?.rejectUnauthorized, true)
  assert.equal(options?.ca, 'test-ca')
  assert.equal(options?.servername, undefined)
  assert.ok(options?.checkServerIdentity)
  assert.equal(
    options.checkServerIdentity(
      '172.16.0.10',
      certificateWithNames('IP Address:203.0.113.10'),
    ),
    undefined,
  )
  assert.ok(
    options.checkServerIdentity(
      '172.16.0.10',
      certificateWithNames('IP Address:172.16.0.10'),
    ) instanceof Error,
  )
})

void test('explicit TLS identity also supports validated DNS and IPv6 names', () => {
  for (const [identity, names] of [
    ['redis.internal.test', 'DNS:redis.internal.test'],
    ['fd00::10', 'IP Address:fd00::10'],
    ['[fd00::10]', 'IP Address:fd00::10'],
  ]) {
    const options = getRedisTlsOptions(Buffer.from('test-ca'), {
      REDIS_URL: ORIGINAL_URL,
      REDIS_TLS_IDENTITY: identity,
    })
    assert.ok(options?.checkServerIdentity)
    assert.equal(
      options.checkServerIdentity('172.16.0.10', certificateWithNames(names)),
      undefined,
    )
  }
})

void test('explicit TLS identity rejects plaintext, missing or empty trust configuration', () => {
  for (const url of [
    undefined,
    '',
    'redis://localhost',
    'rediss:///4',
    'https://example.invalid',
  ]) {
    assert.throws(
      () =>
        getRedisTlsOptions('test-ca', {
          REDIS_URL: url,
          REDIS_TLS_IDENTITY: '203.0.113.10',
        }),
      /^Error: REDIS_TLS_IDENTITY requires a rediss URL and a non-empty CA$/v,
    )
  }

  for (const ca of [undefined, '', '   ', Buffer.alloc(0)]) {
    assert.throws(
      () =>
        getRedisTlsOptions(ca, {
          REDIS_URL: ORIGINAL_URL,
          REDIS_TLS_IDENTITY: '203.0.113.10',
        }),
      /^Error: REDIS_TLS_IDENTITY requires a rediss URL and a non-empty CA$/v,
    )
  }
})

void test('explicit TLS identity rejects URL payloads and malformed names', () => {
  for (const identity of [
    'user@host',
    'host:6379',
    'https://host',
    'host/path',
    'host?query',
    'host#fragment',
    'host_name',
    'host name',
  ]) {
    assert.throws(
      () =>
        getRedisTlsOptions('test-ca', {
          REDIS_URL: ORIGINAL_URL,
          REDIS_TLS_IDENTITY: identity,
        }),
      /^Error: REDIS_TLS_IDENTITY must contain only a hostname or IP address$/v,
    )
  }
})
