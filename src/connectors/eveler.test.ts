import assert from 'node:assert/strict'
import {mkdtemp, rm, stat, readdir} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import {join} from 'node:path'
import test from 'node:test'
import {
  aggregateEvelerHours,
  EvelerClient as ActualEvelerClient,
  EvelerConnector,
  evelerWindows,
} from './eveler.js'

class EvelerClient extends ActualEvelerClient {
  constructor(
    options: ConstructorParameters<typeof ActualEvelerClient>[0] = {},
  ) {
    super({wait: () => Promise.resolve(), ...options})
  }
}
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  type ConnectorRunContext,
} from './types.js'

const window = {
  start: new Date('2026-03-29T00:00:00Z'),
  end: new Date('2026-03-29T01:00:00Z'),
}
const requestUrl = (input: Parameters<typeof fetch>[0]) =>
  input instanceof Request ? input.url : String(input)
const rows = (start = window.start.getTime(), value = 0.0001) =>
  Array.from({length: 6}, (_, index) => ({
    date: new Date(start + (index + 1) * 600_000).toISOString(),
    value,
  }))
const sourceMeterId = '000000000000000000000001'
const response = (values: unknown[] = rows()) => ({
  success: true,
  data: {
    meter_id: sourceMeterId,
    start_date: '2026-03-29T00:00:00Z',
    end_date: '2026-03-30T00:00:00Z',
    unit: 'm3',
    timestep: 600,
    values,
  },
})
const context: ConnectorRunContext = {
  serviceAccount: 'synthetic',
  sourcePointId: 'synthetic-meter',
  connectorId: 'synthetic-connector',
  rate: 100,
  mostRecentAvailableDate: undefined,
  connectorParameters: {
    sourceStartDate: window.start.toISOString(),
    sourceMeterId,
  },
}

void test('Eveler sums six interval-end samples exactly, including zero and UTC/DST offsets', () => {
  const data = rows()
  data[5].date = '2026-03-29T03:00:00+02:00'
  const parsed = aggregateEvelerHours(data, window)
  assert.equal(parsed.values[0].value, 0.0006)
  assert.deepEqual(parsed.values[0], {
    date: window.start,
    periodStart: window.start,
    periodEnd: window.end,
    value: 0.0006,
  })
  assert.equal(
    aggregateEvelerHours(rows(window.start.getTime(), 0), window).values[0]
      .value,
    0,
  )
  assert.equal(parsed.report.completeHours, 1)
})

void test('Eveler deduplicates equal samples and excludes incomplete/conflicting/negative/invalid hours', () => {
  assert.equal(
    aggregateEvelerHours([...rows(), rows()[0]], window).values.length,
    1,
  )
  assert.equal(
    aggregateEvelerHours([...rows(), rows()[0]], window).report.duplicateRows,
    1,
  )
  for (const values of [
    rows().slice(1),
    [...rows(), {...rows()[0], value: 2}],
    [...rows(), {...rows()[0], value: -1}],
    [...rows(), {...rows()[0], value: '0'}],
    [...rows(), {...rows()[0], value: NaN}],
    [...rows(), {...rows()[0], value: 0.00001}],
    [...rows(), {date: '2026-03-29T00:11:00Z', value: 1}],
  ]) {
    const parsed = aggregateEvelerHours(values, window)
    assert.equal(parsed.values.length, 0)
    assert.equal(parsed.report.incompleteHours, 1)
  }
  const parsed = aggregateEvelerHours(
    [
      ...rows(),
      {date: 'no-date', value: 2},
      {date: '2026-03-29T00:00:00Z', value: 9},
      {date: '2026-03-29T01:10:00Z', value: 9},
    ],
    window,
  )
  assert.equal(parsed.report.invalidRows, 1)
  assert.equal(parsed.report.outOfWindowRows, 2)
  assert.equal(parsed.values[0].value, 0.0006)
})

void test('Eveler leaves a gap and never interpolates a missing hour', () => {
  const parsed = aggregateEvelerHours(
    [...rows(), ...rows(window.start.getTime() + 2 * 3_600_000)],
    {...window, end: new Date('2026-03-29T03:00:00Z')},
  )
  assert.equal(parsed.values.length, 2)
  assert.equal(parsed.report.incompleteHours, 1)
  assert.equal(
    parsed.values[1].periodStart?.toISOString(),
    '2026-03-29T02:00:00.000Z',
  )
})

void test('Eveler authenticates with empty POST, raw Authorization, UTC dates and precision 4', async () => {
  const calls: Array<{url: URL; init?: RequestInit}> = []
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: (input, init) => {
      const url = new URL(requestUrl(input))
      calls.push({url, init})
      return Promise.resolve(
        url.pathname.endsWith('/login')
          ? Response.json({success: true, data: {token: 'synthetic-token'}})
          : Response.json(response()),
      )
    },
  })
  await client.fetchVolume(
    'synthetic-meter',
    new Date('2026-03-29T00:00:00Z'),
    new Date('2026-03-30T00:00:00Z'),
    sourceMeterId,
  )
  await client.fetchVolume(
    'synthetic-meter',
    new Date('2026-03-29T00:00:00Z'),
    new Date('2026-03-30T00:00:00Z'),
    sourceMeterId,
  )
  assert.equal(calls.length, 3)
  assert.equal(calls[0].init?.method, 'POST')
  assert.equal(calls[0].init?.body, undefined)
  assert.equal(calls[0].url.searchParams.get('token'), 'synthetic-id')
  assert.equal(calls[0].url.searchParams.get('secret'), 'synthetic-secret')
  assert.equal(
    calls[1].url.pathname,
    '/api/client/meter/synthetic-meter/data/volume/2026-03-29/2026-03-30/4',
  )
  assert.equal(
    new Headers(calls[1].init?.headers).get('Authorization'),
    'synthetic-token',
  )
  for (const call of calls) {
    assert.equal(call.init?.redirect, 'error')
    assert.ok(call.init?.signal)
  }
})

void test('Eveler quota/unavailability/auth/network errors remain bounded and redact secrets', async () => {
  for (const status of [429, 503]) {
    let calls = 0
    const client = new EvelerClient({
      identifier: 'synthetic-id',
      secret: 'synthetic-secret',
      fetcher: () => {
        calls++
        return Promise.resolve(new Response('synthetic-secret', {status}))
      },
    })
    await assert.rejects(
      client.fetchVolume(
        'synthetic-meter',
        window.start,
        new Date('2026-03-30'),
        sourceMeterId,
      ),
      (error: Error) => {
        assert.match(error.message, new RegExp(String(status)))
        assert.doesNotMatch(error.message, /synthetic-secret|synthetic-id/)
        return true
      },
    )
    assert.equal(calls, 1)
  }
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: () => {
      throw new Error('https://api.eveler.pro/?secret=synthetic-secret')
    },
  })
  await assert.rejects(
    client.fetchVolume(
      'synthetic-meter',
      window.start,
      new Date('2026-03-30'),
      sourceMeterId,
    ),
    /Provider request failed or timed out\.$/,
  )
  assert.throws(
    () => new EvelerClient({baseUrl: 'https://third-party.invalid'}),
    /EVELER_API_BASE_URL/,
  )
})

void test('Eveler refreshes once on 401 and rejects invalid response units/timestep/identity', async () => {
  let requests = 0
  let logins = 0
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: (input) => {
      if (requestUrl(input).includes('/login')) {
        logins++
        return Promise.resolve(
          Response.json({success: true, data: {token: 'synthetic-token'}}),
        )
      }
      requests++
      return Promise.resolve(
        new Response('synthetic-private-body', {status: 401}),
      )
    },
  })
  await assert.rejects(
    client.fetchVolume(
      'synthetic-meter',
      window.start,
      new Date('2026-03-30'),
      sourceMeterId,
    ),
    /HTTP 401/,
  )
  assert.equal(logins, 2)
  assert.equal(requests, 2)
  for (const override of [
    {unit: 'L'},
    {timestep: 900},
    {meter_id: '000000000000000000000002'},
    {values: null},
  ]) {
    const provider = new EvelerClient({
      identifier: 'synthetic-id',
      secret: 'synthetic-secret',
      fetcher: (input) =>
        Promise.resolve(
          requestUrl(input).includes('/login')
            ? Response.json({success: true, data: {token: 'synthetic-token'}})
            : Response.json({
                ...response(),
                data: {...response().data, ...override},
              }),
        ),
    })
    await assert.rejects(
      provider.fetchVolume(
        'synthetic-meter',
        window.start,
        new Date('2026-03-30'),
        sourceMeterId,
      ),
      /Invalid volume response/,
    )
  }
})

void test('Eveler renews expiring auth and never retries volume quota errors', async () => {
  let now = new Date('2026-01-01T00:00:00Z')
  let logins = 0
  let requests = 0
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    now: () => now,
    fetcher: (input) => {
      if (requestUrl(input).includes('/login')) {
        logins++
        return Promise.resolve(
          Response.json({success: true, data: {token: 'synthetic-token'}}),
        )
      }
      requests++
      return Promise.resolve(
        new Response('private-provider-details', {status: 429}),
      )
    },
  })
  const read = () =>
    client.fetchVolume(
      'synthetic-meter',
      window.start,
      new Date('2026-03-30'),
      sourceMeterId,
    )
  await assert.rejects(read(), /HTTP 429/)
  assert.equal(logins, 1)
  assert.equal(requests, 1)
  now = new Date('2026-01-01T00:56:00Z')
  await assert.rejects(read(), /HTTP 429/)
  assert.equal(logins, 2)
  assert.equal(requests, 2)
})

void test('Eveler spaces authentication, data and retry requests by at least 1100 ms', async () => {
  let clock = 0
  const requestTimes: number[] = []
  let dataCalls = 0
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    requestClock: () => clock,
    wait: (milliseconds) => {
      clock += milliseconds
      return Promise.resolve()
    },
    fetcher: (input) => {
      requestTimes.push(clock)
      if (requestUrl(input).includes('/login'))
        return Promise.resolve(
          Response.json({success: true, data: {token: 'synthetic-token'}}),
        )
      dataCalls++
      return Promise.resolve(
        dataCalls === 1
          ? new Response(null, {status: 401})
          : Response.json(response()),
      )
    },
  })
  await client.fetchVolume(
    'synthetic-meter',
    window.start,
    new Date('2026-03-30'),
    sourceMeterId,
  )
  assert.deepEqual(requestTimes, [0, 1100, 2200, 3300])
})

void test('Eveler preserves a raw response before rejecting its contract, without another provider request', async (t) => {
  const cacheDirectory = await mkdtemp(join(tmpdir(), 'eveler-invalid-unit-'))
  t.after(() => rm(cacheDirectory, {recursive: true, force: true}))
  let calls = 0
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: (input) => {
      calls++
      return Promise.resolve(
        requestUrl(input).includes('/login')
          ? Response.json({success: true, data: {token: 'synthetic-token'}})
          : Response.json({
              ...response(),
              data: {...response().data, unit: 'L'},
            }),
      )
    },
  })
  await assert.rejects(
    new EvelerConnector({client, window, cacheDirectory}).run(context),
    /Invalid volume response/,
  )
  assert.equal((await readdir(cacheDirectory)).length, 1)
  await assert.rejects(
    new EvelerConnector({window, cacheDirectory, cacheOnly: true}).run(context),
    /Invalid or unreadable raw cache/,
  )
  assert.equal(calls, 2)
})

void test('Eveler explicit replay includes the final hourly sample, caches raw response and reuses it offline', async (t) => {
  const cacheDirectory = await mkdtemp(join(tmpdir(), 'eveler-unit-'))
  t.after(() => rm(cacheDirectory, {recursive: true, force: true}))
  let calls = 0
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: (input) => {
      calls++
      const url = requestUrl(input)
      if (url.includes('/login'))
        return Promise.resolve(
          Response.json({success: true, data: {token: 'synthetic-token'}}),
        )
      assert.ok(url.includes('/2026-03-29/2026-03-30/4'))
      return Promise.resolve(Response.json(response()))
    },
  })
  const first = await new EvelerConnector({client, window, cacheDirectory}).run(
    context,
  )
  const second = await new EvelerConnector({
    window,
    cacheDirectory,
    cacheOnly: true,
  }).run(context)
  assert.equal(calls, 2)
  assert.deepEqual(first.data, second.data)
  assert.equal(first.data.metrics[0].type, MetricType.VOLUME)
  assert.equal(first.data.metrics[0].unit, MetricUnit.M3)
  assert.equal(first.data.metrics[0].granularity, Granularity.HOUR)
  assert.equal(
    first.data.metrics[0].conflictPolicy,
    ConflictPolicy.SKIP_CONFLICTING_VALUES,
  )
  const files = await readdir(cacheDirectory)
  assert.equal(files.length, 1)
  assert.equal((await stat(join(cacheDirectory, files[0]))).mode & 0o777, 0o600)
  await assert.rejects(
    new EvelerConnector({window, cacheDirectory, cacheOnly: true}).run({
      ...context,
      connectorParameters: {
        ...context.connectorParameters,
        sourceMeterId: '000000000000000000000002',
      },
    }),
    /Invalid or unreadable raw cache/,
  )
  await assert.rejects(
    new EvelerConnector({window, cacheDirectory, cacheOnly: true}).run({
      ...context,
      sourcePointId: 'other-synthetic-meter',
    }),
    /cache entry is missing/,
  )
})

void test('Eveler resumes seven days before the cursor, clamps to source start and closes the current hour', async () => {
  const client = new EvelerClient({
    identifier: 'synthetic-id',
    secret: 'synthetic-secret',
    fetcher: (input) =>
      Promise.resolve(
        requestUrl(input).includes('/login')
          ? Response.json({success: true, data: {token: 'synthetic-token'}})
          : Response.json(response()),
      ),
  })
  const output = await new EvelerConnector({
    client,
    now: () => new Date('2026-04-10T14:35:00Z'),
  }).run({
    ...context,
    mostRecentAvailableDate: new Date('2026-04-09T14:00:00Z'),
  })
  assert.equal(
    output.data.source_metadata?.requestedStart,
    '2026-04-02T14:00:00.000Z',
  )
  assert.equal(
    output.data.source_metadata?.requestedEnd,
    '2026-04-10T14:00:00.000Z',
  )
  await assert.rejects(
    new EvelerConnector({client, window}).run({
      ...context,
      connectorParameters: {sourceMeterId},
    }),
    /sourceStartDate/,
  )
  await assert.rejects(
    new EvelerConnector({client, window}).run({
      ...context,
      connectorParameters: {sourceStartDate: window.start.toISOString()},
    }),
    /sourceMeterId/,
  )
  const windows = evelerWindows({
    start: new Date('2020-01-01'),
    end: new Date('2023-01-01'),
  })
  assert.equal(windows.length, 4)
  assert.equal(windows[1].start.getTime(), windows[0].end.getTime())
  assert.throws(
    () => evelerWindows({start: window.end, end: window.start}),
    /bounded/,
  )
})
