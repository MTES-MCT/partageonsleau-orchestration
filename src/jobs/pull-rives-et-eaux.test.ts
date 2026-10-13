/* eslint-disable @typescript-eslint/require-await -- Test doubles preserve asynchronous connector contracts. */
import assert from 'node:assert/strict'
import test from 'node:test'
import {
  MeterReadingsClient,
  type MeterStreamContext,
  type MeterIngestionBatch,
  type MeterIngestionResult,
} from '../services/meter-readings-client.js'
import {pullRivesEtEaux} from './pull-rives-et-eaux.js'

const context: MeterStreamContext = {
  provider: 'rives-et-eaux',
  scope: 'epidropt',
  streams: [
    {
      id: 'stream1',
      externalId: 'meter1',
      compteurId: 'compteur1',
      activatedAt: '2026-09-17T00:00:00Z',
    },
    {
      id: 'stream2',
      externalId: 'meter2',
      compteurId: 'compteur2',
      activatedAt: '2026-09-17T00:00:00Z',
    },
  ],
}
const counts = {
  received: 0,
  accepted: 0,
  blocked: 0,
  unknownMeters: 0,
  unchanged: 0,
  published: 0,
  conflicts: 0,
}
const now = new Date('2026-09-17T10:00:00Z')

function acknowledgment(batch: MeterIngestionBatch): MeterIngestionResult {
  return {
    persisted: true,
    ingestionId: 'batch',
    checkpoint: batch.windowEnd,
    counts: {...counts, received: batch.readings.length},
  }
}

void test('disabled Rives job and empty authorized context never fetch provider', async () => {
  await pullRivesEtEaux({
    enabled: false,
    client: {
      async getContext() {
        throw new Error('Must not authenticate')
      },
      async ingest(batch) {
        return acknowledgment(batch)
      },
    },
  })
  await pullRivesEtEaux({
    enabled: true,
    now,
    client: {
      async getContext() {
        return {...context, streams: []}
      },
      async ingest(batch) {
        return acknowledgment(batch)
      },
    },
    connector: {
      async fetchWindow() {
        throw new Error('Must not fetch')
      },
    },
  })
})

void test('daily batch job fetches 15 windows once regardless of meter count and awaits durable ingestion', async () => {
  const batches: MeterIngestionBatch[] = []
  const events: string[] = []
  const client = {
    async getContext(provider: string, scope: string) {
      assert.equal(provider, 'rives-et-eaux')
      assert.equal(scope, 'epidropt')
      return context
    },
    async ingest(batch: MeterIngestionBatch) {
      events.push(`ingest:${batches.length}`)
      batches.push(batch)
      return acknowledgment(batch)
    },
  }
  let fetchCount = 0
  await pullRivesEtEaux({
    enabled: true,
    now,
    client,
    connector: {
      async fetchWindow() {
        events.push(`fetch:${fetchCount++}`)
        return [
          {
            NumeroSerieCompteur: 'meter1',
            Date: '2026-08-19T00:00:00',
            Index: 10,
            CodeValidite: 'Y',
          },
        ]
      },
    },
  })
  assert.equal(fetchCount, 15)
  assert.equal(batches.length, 15)
  assert.deepEqual(events.slice(0, 4), [
    'fetch:0',
    'ingest:0',
    'fetch:1',
    'ingest:1',
  ])
  assert.equal(batches[0].readings.length, 1)
  assert.equal(batches[1].readings.length, 0)
  assert.equal(batches[0].mode, 'LIVE')
  assert.equal(batches[0].provider, 'rives-et-eaux')
  assert.equal(batches[0].scope, 'epidropt')
  assert.equal(batches[0].readings[0].status, 'INVALID')
  assert.equal(batches[0].readings[0].reason, 'EXCLUDED_QUALITY')
  assert.equal(batches[0].readings[0].index, '10')
  assert.equal(batches[0].complete, true)
  assert.equal(batches[0].windowStart, '2026-08-17T22:00:00.000Z')
  assert.equal(batches.at(-1)?.windowEnd, '2026-09-01T22:00:00.000Z')
})

void test('Rives excludes the exact maturity cutoff and newer observations, preserving zero, quality and invalid dates', async () => {
  const batches: MeterIngestionBatch[] = []
  const mature = {
    NumeroSerieCompteur: 'meter1',
    Date: '2026-09-01T23:59:59.999',
    Index: 0,
    CodeValidite: 'B',
  }
  const invalidDate = {...mature, Date: 'not-a-date'}
  await pullRivesEtEaux({
    enabled: true,
    now,
    client: {
      async getContext() {
        return context
      },
      async ingest(batch) {
        batches.push(batch)
        return acknowledgment(batch)
      },
    },
    connector: {
      async fetchWindow(window) {
        return window.localStart === '2026-09-01'
          ? [
              mature,
              {...mature, CodeValidite: 'Y'},
              {...mature, Date: '2026-09-02T00:00:00'},
              {...mature, Date: '2026-09-02T00:00:00.001'},
              {...mature, Date: '2026-09-16T12:00:00'},
              {...mature, Date: '2026-09-16T12:00:00', CodeValidite: 'Y'},
              invalidDate,
            ]
          : []
      },
    },
  })
  const readings = batches.flatMap((batch) => batch.readings)
  assert.equal(readings.length, 3)
  assert.equal(readings[0].observedAt, '2026-09-01T21:59:59.999Z')
  assert.equal(readings[0].index, '0')
  assert.equal(readings[0].quality, 'B')
  assert.equal(readings[0].status, 'VALID')
  assert.equal(readings[1].reason, 'EXCLUDED_QUALITY')
  assert.equal(readings[1].index, '0')
  assert.equal(readings[2].observedAt, null)
  assert.equal(readings[2].status, 'INVALID')
  assert.equal(readings[2].reason, 'INVALID_LOCAL_TIME')
  assert.deepEqual(readings[2].raw, invalidDate)
})

void test('inclusive provider boundaries are deduplicated and the withheld boundary becomes eligible the next day', async () => {
  async function runDay(at: Date) {
    const batches: MeterIngestionBatch[] = []
    await pullRivesEtEaux({
      enabled: true,
      now: at,
      client: {
        async getContext() {
          return context
        },
        async ingest(batch) {
          batches.push(batch)
          return acknowledgment(batch)
        },
      },
      connector: {
        async fetchWindow(window) {
          return [window.localStart, window.localEnd].map((day) => ({
            NumeroSerieCompteur: 'meter1',
            Date: `${day}T00:00:00`,
            Index: 0,
            CodeValidite: 'A',
          }))
        },
      },
    })
    return batches.flatMap((batch) => batch.readings)
  }
  const first = await runDay(now)
  const next = await runDay(new Date('2026-09-18T10:00:00Z'))
  const firstTimes = first.map((reading) => reading.observedAt)
  const nextTimes = next.map((reading) => reading.observedAt)
  assert.equal(first.length, 15)
  assert.equal(new Set(firstTimes).size, 15)
  assert.equal(next.length, 15)
  assert.equal(new Set(nextTimes).size, 15)
  assert.deepEqual(firstTimes.slice(1), nextTimes.slice(0, -1))
  assert.equal(firstTimes.includes('2026-09-01T22:00:00.000Z'), false)
  assert.equal(nextTimes.at(-1), '2026-09-01T22:00:00.000Z')
  assert.ok([...first, ...next].every((reading) => reading.index === '0'))
})

void test('the Rives maturity cutoff is fixed before authentication and survives a job crossing Paris midnight', async (t) => {
  t.mock.timers.enable({
    apis: ['Date'],
    now: new Date('2026-09-16T21:59:59.999Z'),
  })
  const batches: MeterIngestionBatch[] = []
  await pullRivesEtEaux({
    enabled: true,
    client: {
      async getContext() {
        t.mock.timers.setTime(new Date('2026-09-16T22:00:00.000Z').getTime())
        return context
      },
      async ingest(batch) {
        batches.push(batch)
        return acknowledgment(batch)
      },
    },
    connector: {
      async fetchWindow(window) {
        t.mock.timers.setTime(new Date('2026-09-17T22:00:00.000Z').getTime())
        return window.localStart === '2026-08-31'
          ? ['2026-08-31T23:59:59.999', '2026-09-01T00:00:00'].map(
              (observedDate) => ({
                NumeroSerieCompteur: 'meter1',
                Date: observedDate,
                Index: 1,
                CodeValidite: 'A',
              }),
            )
          : []
      },
    },
  })
  assert.equal(batches.length, 15)
  assert.equal(batches[0].windowStart, '2026-08-16T22:00:00.000Z')
  assert.equal(batches.at(-1)?.windowEnd, '2026-08-31T22:00:00.000Z')
  assert.deepEqual(
    batches.flatMap((batch) =>
      batch.readings.map((reading) => reading.observedAt),
    ),
    ['2026-08-31T21:59:59.999Z'],
  )
})

void test('a retry after a middle-window failure rereads mature windows without losing the previous successes', async () => {
  const attempted: MeterIngestionBatch[] = []
  let fail = true
  const options = {
    enabled: true,
    now,
    client: {
      async getContext() {
        return context
      },
      async ingest(batch: MeterIngestionBatch) {
        attempted.push(batch)
        if (fail && attempted.length === 4) {
          throw new Error('Transient ingestion failure')
        }
        return acknowledgment(batch)
      },
    },
    connector: {
      async fetchWindow() {
        return []
      },
    },
  }
  await assert.rejects(pullRivesEtEaux(options), /Transient ingestion failure/v)
  assert.equal(attempted.length, 4)
  fail = false
  await pullRivesEtEaux(options)
  const replay = attempted.slice(4)
  assert.equal(replay.length, 15)
  assert.deepEqual(
    replay.slice(0, 4).map((batch) => [batch.windowStart, batch.windowEnd]),
    attempted.slice(0, 4).map((batch) => [batch.windowStart, batch.windowEnd]),
  )
  assert.equal(replay.at(-1)?.windowEnd, '2026-09-01T22:00:00.000Z')
})

void test('provider and ingestion failures propagate, stop following windows and are retryable by BullMQ', async () => {
  let ingested = 0
  const client = {
    async getContext() {
      return context
    },
    async ingest(batch: MeterIngestionBatch) {
      ingested++
      return acknowledgment(batch)
    },
  }
  await assert.rejects(
    pullRivesEtEaux({
      enabled: true,
      now,
      client,
      connector: {
        async fetchWindow() {
          throw new Error('Provider failed')
        },
      },
    }),
    /Provider failed/v,
  )
  assert.equal(ingested, 0)
  let fetched = 0
  await assert.rejects(
    pullRivesEtEaux({
      enabled: true,
      now,
      client: {
        ...client,
        async ingest() {
          throw new Error('No durable checkpoint')
        },
      },
      connector: {
        async fetchWindow() {
          fetched++
          return []
        },
      },
    }),
    /No durable checkpoint/v,
  )
  assert.equal(fetched, 1)
})

void test('PLE client requires durable acknowledgment including checkpoint and received count', async (t) => {
  const timeouts: number[] = []
  t.mock.method(AbortSignal, 'timeout', (milliseconds: number) => {
    timeouts.push(milliseconds)
    return new AbortController().signal
  })
  const batch: MeterIngestionBatch = {
    provider: 'rives-et-eaux',
    scope: 'epidropt',
    batchId: 'batch',
    windowStart: '2026-09-01T22:00:00.000Z',
    windowEnd: '2026-09-02T22:00:00.000Z',
    fetchedAt: now.toISOString(),
    complete: true,
    mode: 'LIVE',
    readings: [
      {
        externalId: null,
        observedAt: null,
        index: null,
        status: 'INVALID',
        reason: 'INVALID_ROW',
        raw: null,
      },
    ],
  }
  const requests: Array<{path: string; options: RequestInit | undefined}> = []
  let reply: unknown = acknowledgment(batch)
  const client = new MeterReadingsClient(
    'https://ple.invalid',
    'sa_test',
    'secret',
    async (input, options) => {
      const path = new URL(
        input instanceof Request ? input.url : input.toString(),
      ).pathname
      requests.push({path, options})
      return Response.json(
        path.endsWith('/token')
          ? {accessToken: 'token'}
          : path.endsWith('/meter-streams')
            ? context
            : reply,
      )
    },
  )
  assert.deepEqual(
    await client.getContext('rives-et-eaux', 'epidropt'),
    context,
  )
  assert.deepEqual(await client.ingest(batch), acknowledgment(batch))
  assert.deepEqual(timeouts, [60_000, 60_000, 150_000])
  assert.equal(
    requests.at(-1)?.path,
    '/service-accounts/meter-readings/ingestions',
  )
  assert.equal(
    new Headers(requests.at(-1)?.options?.headers).get('Authorization'),
    'Bearer token',
  )
  for (reply of [
    {...acknowledgment(batch), persisted: false},
    {...acknowledgment(batch), checkpoint: 'later'},
    {...acknowledgment(batch), counts},
    {success: true},
  ]) {
    await assert.rejects(client.ingest(batch), /durable complete ingestion/v)
  }
  assert.deepEqual(timeouts.slice(2), Array<number>(5).fill(150_000))
})

void test('meter ingestion timeout is sanitized and stops the job without acknowledging or fetching another window', async (t) => {
  const controller = new AbortController()
  const timeoutError = new Error('test-secret test-token timeout detail')
  const timeouts: number[] = []
  t.mock.method(AbortSignal, 'timeout', (milliseconds: number) => {
    timeouts.push(milliseconds)
    return milliseconds === 150_000
      ? controller.signal
      : new AbortController().signal
  })
  let ingestions = 0
  let fetched = 0
  const client = new MeterReadingsClient(
    'https://ple.invalid',
    'sa_test',
    'test-secret',
    async (input, options) => {
      const path = new URL(
        input instanceof Request ? input.url : input.toString(),
      ).pathname
      if (path.endsWith('/token')) {
        return Response.json({accessToken: 'test-token'})
      }
      if (path.endsWith('/meter-streams')) {
        return Response.json(context)
      }
      ingestions++
      assert.equal(options?.method, 'POST')
      assert.equal(options.signal, controller.signal)
      assert.equal(options.redirect, 'error')
      return new Promise<Response>((_resolve, reject) => {
        controller.signal.addEventListener(
          'abort',
          () => reject(timeoutError),
          {once: true},
        )
        queueMicrotask(() => controller.abort(timeoutError))
      })
    },
  )
  await assert.rejects(
    pullRivesEtEaux({
      enabled: true,
      now,
      client,
      connector: {
        async fetchWindow() {
          fetched++
          return []
        },
      },
    }),
    {
      message:
        '[meter-readings] PLE request /service-accounts/meter-readings/ingestions failed or timed out.',
    },
  )
  assert.deepEqual(timeouts, [60_000, 60_000, 150_000])
  assert.equal(ingestions, 1)
  assert.equal(fetched, 1)
})

void test('PLE configuration never falls back to mocks and failures remain sanitized', async () => {
  assert.throws(() => new MeterReadingsClient('', '', ''), /required/v)
  const client = new MeterReadingsClient(
    'https://ple.invalid',
    'sa_test',
    'test-secret',
    async () => new Response('test-secret', {status: 401}),
  )
  await assert.rejects(
    client.getContext('rives-et-eaux', 'epidropt'),
    (error: Error) =>
      error.message.includes('401') && !error.message.includes('test-secret'),
  )
})

void test('meter transport accepts another provider without timezone or provider-specific fields', async () => {
  const provider = 'synthetic-provider'
  const scope = 'test scope & boundary'
  const genericContext = {...context, provider, scope}
  const requested: URL[] = []
  let sent: unknown
  const client = new MeterReadingsClient(
    'https://ple.invalid',
    'client',
    'secret',
    async (input, options) => {
      const url = new URL(
        input instanceof Request ? input.url : input.toString(),
      )
      requested.push(url)
      if (url.pathname.endsWith('/token')) {
        return Response.json({accessToken: 'token'})
      }

      if (url.pathname.endsWith('/meter-streams')) {
        return Response.json(genericContext)
      }

      assert.equal(typeof options?.body, 'string')
      if (typeof options?.body !== 'string') {
        throw new TypeError('Expected a JSON request body')
      }

      sent = JSON.parse(options.body) as unknown
      return Response.json(acknowledgment(batch))
    },
  )
  assert.deepEqual(await client.getContext(provider, scope), genericContext)
  assert.equal(requested[1].pathname, '/service-accounts/meter-streams')
  assert.equal(requested[1].searchParams.get('provider'), provider)
  assert.equal(requested[1].searchParams.get('scope'), scope)
  const batch: MeterIngestionBatch = {
    provider,
    scope,
    batchId: 'other-provider',
    complete: true,
    mode: 'LIVE',
    windowStart: '2026-09-01T00:00:00Z',
    windowEnd: '2026-09-02T00:00:00Z',
    fetchedAt: now.toISOString(),
    readings: [
      {
        externalId: 'meter',
        observedAt: '2026-09-01T12:34:56+00:00',
        index: '123.4567',
        status: 'VALID',
        quality: 'custom-code',
        raw: {arbitrary: 'format'},
      },
    ],
  }
  await client.ingest(batch)
  assert.deepEqual(sent, batch)
})

void test('meter transport rejects missing scope and a mismatched authorized context', async () => {
  let calls = 0
  const client = new MeterReadingsClient(
    'https://ple.invalid',
    'client',
    'secret',
    async (input) => {
      calls++
      const url = new URL(
        input instanceof Request ? input.url : input.toString(),
      )
      return Response.json(
        url.pathname.endsWith('/token')
          ? {accessToken: 'token'}
          : {...context, scope: 'wrong-scope'},
      )
    },
  )
  await assert.rejects(
    client.getContext('rives-et-eaux', ''),
    /scope are required/v,
  )
  assert.equal(calls, 0)
  await assert.rejects(
    client.getContext('rives-et-eaux', 'epidropt'),
    /Invalid PLE stream context/v,
  )
})
