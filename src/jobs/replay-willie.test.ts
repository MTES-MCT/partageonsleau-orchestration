import assert from 'node:assert/strict'
import test, {type TestContext} from 'node:test'
import {createHash} from 'node:crypto'
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import {
  PointFlowType,
  type ConnectorOutput,
  type ServiceAccountPointContext,
} from '../connectors/types.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'
import {
  applyWillieReplay,
  parseCachedWillieDays,
  prepareWillieReplay,
  validateWillieReplayPlan,
  willieParisMidnight,
  willieReplayDigest,
} from './replay-willie.js'

const ids = {
  declarant: '11111111-1111-4111-a111-111111111111',
  connector: '22222222-2222-4222-a222-222222222222',
  point: '33333333-3333-4333-a333-333333333333',
  exploitation: '44444444-4444-4444-a444-444444444444',
  station: '55555555-5555-4555-a555-555555555555',
}
const window = {start: '2026-01-01', end: '2026-01-04'}
const now = new Date('2026-10-12T00:00:00Z')
const bytesHash = (value: string) =>
  createHash('sha256').update(value).digest('hex')

function configure(t: TestContext) {
  const settings = {
    PLE_BASE_URL: 'https://synthetic-api.example.test',
    CLIENT_ID: 'sa_synthetic',
    CLIENT_SECRET: 'synthetic-not-a-secret',
  }
  const previous = Object.fromEntries(
    Object.keys(settings).map((key) => [key, process.env[key]]),
  )
  Object.assign(process.env, settings)
  t.after(() => {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key]
      else process.env[key] = value
    }
  })
  const fetchMock = t.mock.method(globalThis, 'fetch')
  fetchMock.mock.mockImplementation(() => {
    throw new Error('PROVIDER_OR_OTHER_NETWORK_FORBIDDEN')
  })
  return fetchMock
}

function response(
  datapoints = [
    {dateTime: '2025-12-31', consumption: 7},
    {dateTime: '2026-01-01', consumption: null as number | null},
    {dateTime: '2026-01-02', consumption: 12.5},
    {dateTime: '2026-01-03', consumption: 0},
  ],
) {
  return {
    stations: [{stationID: ids.station, datapoints}],
    count: 1,
    unknownStationIds: [],
  }
}

async function fixture(t: TestContext, body: unknown = response()) {
  const fetchMock = configure(t)
  const directory = await mkdtemp(
    path.join(tmpdir(), 'willie-replay-synthetic-'),
  )
  t.after(() => rm(directory, {recursive: true, force: true}))
  const rawFile = `${ids.station}.response.json`
  const bytes = JSON.stringify(body)
  const metadata = {
    version: 1,
    provider: 'willie',
    endpoint: 'https://api.meetwillie.com/v1/stations/consumption',
    method: 'GET',
    resolution: 'day',
    httpStatus: 200,
    rawFile,
    bodySha256: bytesHash(bytes),
    summary: {requestedStationId: ids.station},
    window: {
      startLocalDate: '2026-01-01',
      endLocalDateExclusive: '2026-10-12',
      timeZone: 'Europe/Paris',
    },
  }
  await Promise.all([
    writeFile(path.join(directory, rawFile), bytes, {mode: 0o600}),
    writeFile(
      path.join(directory, `${ids.station}.metadata.json`),
      JSON.stringify(metadata),
      {mode: 0o600},
    ),
  ])
  const point: ServiceAccountPointContext = {
    pointId: ids.point,
    exploitationId: ids.exploitation,
    connectorId: ids.connector,
    sourcePointId: ids.station,
    connector: 'willie',
    connectorRate: 50,
    flowType: PointFlowType.PRELEVEMENT,
    mostRecentAvailableDate: now,
  }
  const sent: ConnectorOutput[] = []
  const state = {
    ack: {imported: true, skippedValues: 0} as
      {imported: boolean; skippedValues: number} | undefined,
  }
  const client = {
    getServiceAccountToken: () =>
      Promise.resolve('synthetic-token-never-stored'),
    getContextsForDeclarant: () =>
      Promise.resolve([{contextId: 'synthetic-context', points: [point]}]),
    ingest: (parameters: {
      output: ConnectorOutput
      requireAcknowledgement?: boolean
    }) => {
      assert.equal(parameters.requireAcknowledgement, true)
      sent.push(parameters.output)
      return Promise.resolve(state.ack)
    },
  }
  const options = {
    target: 'prod' as const,
    connectorId: ids.connector,
    declarantId: ids.declarant,
    window,
    cacheDirectory: directory,
  }
  return {directory, metadata, point, sent, state, client, options, fetchMock}
}

void test('Willie prepare remains cached-only/read-only, freezes target and payload, and applies identical data', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  assert.equal(f.sent.length, 0)
  assert.deepEqual(plan.summary, {
    numericDays: 2,
    nullDays: 1,
    outsideWindow: 1,
  })
  assert.equal(plan.binding.connectorRate, 50)
  assert.equal(plan.output.data.metrics[0].values[0].value, 12.5)
  assert.equal(
    plan.output.data.metrics[0].conflictPolicy,
    'SKIP_CONFLICTING_VALUES',
  )
  assert.equal(
    plan.output.data.metrics[0].values[0].periodStart,
    '2026-01-02T00:00:00.000Z',
  )
  assert.equal(
    plan.output.data.metrics[0].values[0].periodEnd,
    '2026-01-03T00:00:00.000Z',
  )
  assert.equal(JSON.stringify(plan).includes('synthetic-token'), false)
  assert.equal(JSON.stringify(plan).includes('synthetic-not-a-secret'), false)
  const receipt = await applyWillieReplay(
    {
      target: 'prod',
      plan: JSON.parse(JSON.stringify(plan)) as unknown,
      expectedPlanHash: plan.planHash,
      cacheDirectory: f.directory,
    },
    f.client,
    now,
  )
  assert.equal(receipt.acknowledgement.imported, true)
  assert.equal(f.sent.length, 1)
  assert.deepEqual(
    JSON.parse(JSON.stringify(f.sent[0])) as unknown,
    plan.output,
  )
  assert.equal(f.sent[0].data.metrics[0].values[1].value, 0)
})

void test('Willie replay explicit periods survive the real client and exact acknowledgement bounds', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  const transport = new PartageonsLeauClient()
  let requests = 0
  f.fetchMock.mock.mockImplementation((url, init) => {
    assert.ok(init)
    assert.equal(
      url,
      'https://synthetic-api.example.test/service-accounts/connectors/ingest',
    )
    assert.equal(init.method, 'POST')
    assert.equal(init.redirect, 'error')
    assert.ok(init.signal)
    assert.ok(typeof init.body === 'string')
    const body = JSON.parse(init.body) as {
      data: typeof plan.output.data
      metadata: {
        point_id: string
        exploitation_id: string
        connector_id: string
        connector_rate: number
      }
    }
    assert.deepEqual(body.data, plan.output.data)
    assert.equal(body.metadata.point_id, ids.point)
    assert.equal(body.metadata.exploitation_id, ids.exploitation)
    assert.equal(body.metadata.connector_id, ids.connector)
    assert.equal(body.metadata.connector_rate, 50)
    requests++
    return Promise.resolve(
      new Response(
        JSON.stringify({
          success: true,
          imported: true,
          sourceId: 'synthetic-source',
          skippedValues: 0,
          minDate: plan.output.data.min_date,
          maxDate: plan.output.data.max_date,
        }),
        {status: 200, headers: {'Content-Type': 'application/json'}},
      ),
    )
  })
  const receipt = await applyWillieReplay(
    {
      target: 'prod',
      plan,
      expectedPlanHash: plan.planHash,
      cacheDirectory: f.directory,
    },
    {...f.client, ingest: transport.ingest.bind(transport)},
    now,
  )
  assert.equal(receipt.acknowledgement.imported, true)
  assert.equal(requests, 1)
})

void test('Willie replay accepts acknowledged idempotent skips despite cursor advancement', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  f.point.mostRecentAvailableDate = new Date('2026-10-13T00:00:00Z')
  f.state.ack = {imported: false, skippedValues: 2}
  const receipt = await applyWillieReplay(
    {
      target: 'prod',
      plan,
      expectedPlanHash: plan.planHash,
      cacheDirectory: f.directory,
    },
    f.client,
    now,
  )
  assert.equal(receipt.acknowledgement.imported, false)
  assert.equal(receipt.acknowledgement.skippedValues, 2)
})

void test('Willie periods preserve legacy UTC labels while provider windows respect French daylight saving', () => {
  for (const [day, next, hours] of [
    ['2026-03-29', '2026-03-30', 23],
    ['2026-10-25', '2026-10-26', 25],
  ] as const) {
    const parsed = parseCachedWillieDays(
      response([{dateTime: day, consumption: 1}]),
      ids.station,
      {start: day, end: next},
    )
    const value = parsed.values[0]
    assert.equal(
      (Date.parse(value.periodEnd) - Date.parse(value.periodStart)) / 3_600_000,
      24,
    )
    assert.equal(value.periodStart, `${day}T00:00:00.000Z`)
    assert.equal(value.periodEnd, `${next}T00:00:00.000Z`)
    assert.equal(
      (willieParisMidnight(next).getTime() -
        willieParisMidnight(day).getTime()) /
        3_600_000,
      hours,
    )
  }
  assert.equal(
    willieParisMidnight('2026-07-01').toISOString(),
    '2026-06-30T22:00:00.000Z',
  )
  assert.equal(
    willieParisMidnight('2026-01-01').toISOString(),
    '2025-12-31T23:00:00.000Z',
  )
})

void test('Willie parser rejects unknown stations, duplicates, malformed dates and invalid consumptions', () => {
  assert.throws(
    () =>
      parseCachedWillieDays(
        {stations: [], unknownStationIds: [ids.station]},
        ids.station,
        window,
      ),
    /WILLIE_REPLAY_STATION_UNKNOWN/,
  )
  assert.throws(
    () =>
      parseCachedWillieDays(
        {stations: [], unknownStationIds: []},
        ids.station,
        window,
      ),
    /WILLIE_REPLAY_RESPONSE_STATION_MISMATCH/,
  )
  assert.throws(
    () =>
      parseCachedWillieDays(
        response([
          {dateTime: '2026-01-02', consumption: 1},
          {dateTime: '2026-01-02', consumption: 1},
        ]),
        ids.station,
        window,
      ),
    /WILLIE_REPLAY_DUPLICATE_DAY/,
  )
  for (const consumption of [-1, Number.NaN, Number.POSITIVE_INFINITY, '12']) {
    const body = {
      stations: [
        {
          stationID: ids.station,
          datapoints: [{dateTime: '2026-01-02', consumption}],
        },
      ],
      unknownStationIds: [],
    }
    assert.throws(
      () => parseCachedWillieDays(body, ids.station, window),
      /WILLIE_REPLAY_CONSUMPTION_INVALID/,
    )
  }
  for (const dateTime of ['2026-02-30', '2026-01-01T00:00:00Z', 'not-a-day']) {
    assert.throws(
      () =>
        parseCachedWillieDays(
          response([{dateTime, consumption: 1}]),
          ids.station,
          window,
        ),
      /WILLIE_REPLAY_DAY_INVALID/,
    )
  }
})

void test('Willie requires explicit bounded complete days and a real HTTPS target', async (t) => {
  const f = await fixture(t)
  for (const invalid of [
    {start: '2026-01-02', end: '2026-01-01'},
    {start: '2024-01-01', end: '2026-01-01'},
    {start: '2026-10-12', end: '2026-10-13'},
  ]) {
    await assert.rejects(
      prepareWillieReplay({...f.options, window: invalid}, f.client, now),
      /WILLIE_REPLAY_(?:WINDOW_INVALID|INCOMPLETE_DAY)/,
    )
  }
  for (const url of [
    'http://localhost:5000',
    'https://secret@example.test',
    'https://example.test?token=secret',
  ]) {
    process.env.PLE_BASE_URL = url
    await assert.rejects(
      prepareWillieReplay(f.options, f.client, now),
      /WILLIE_REPLAY_TARGET_INVALID/,
    )
  }
  assert.equal(f.sent.length, 0)
})

void test('Willie apply rejects changed target, source station, rate, exploitation or counting code', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  const apply = () =>
    applyWillieReplay(
      {
        target: 'prod',
        plan,
        expectedPlanHash: plan.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    )
  process.env.PLE_BASE_URL = 'https://other.example.test'
  await assert.rejects(apply(), /WILLIE_REPLAY_TARGET_CHANGED/)
  process.env.PLE_BASE_URL = 'https://synthetic-api.example.test'
  for (const changes of [
    {connectorRate: 100},
    {exploitationId: ids.point},
    {sourcePointId: ids.connector},
    {countingCode: 'NEW'},
  ]) {
    const original = {...f.point}
    Object.assign(f.point, changes)
    await assert.rejects(apply(), /WILLIE_REPLAY_MAPPING_CHANGED/)
    Object.assign(f.point, original)
    delete f.point.countingCode
  }
  await assert.rejects(
    applyWillieReplay(
      {
        target: 'testing',
        plan,
        expectedPlanHash: plan.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    ),
    /WILLIE_REPLAY_TARGET_CHANGED/,
  )
  assert.equal(f.sent.length, 0)
})

void test('Willie cache hashes and approved plan hash prevent silent changes', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  const changed = structuredClone(plan)
  changed.output.data.metrics[0].values[0].value = 999
  assert.throws(
    () => validateWillieReplayPlan(changed, plan.planHash),
    /WILLIE_REPLAY_PLAN_HASH_MISMATCH/,
  )
  const {planHash: _hash, ...payload} = changed
  assert.equal(_hash, plan.planHash)
  changed.planHash = willieReplayDigest(payload)
  await assert.rejects(
    applyWillieReplay(
      {
        target: 'prod',
        plan: changed,
        expectedPlanHash: changed.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    ),
    /WILLIE_REPLAY_PAYLOAD_CHANGED/,
  )
  const rawPath = path.join(f.directory, `${ids.station}.response.json`)
  await writeFile(rawPath, (await readFile(rawPath, 'utf8')) + '\n')
  await assert.rejects(
    applyWillieReplay(
      {
        target: 'prod',
        plan,
        expectedPlanHash: plan.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    ),
    /WILLIE_REPLAY_CACHE_INVALID/,
  )
  assert.equal(f.sent.length, 0)
})

void test('Willie cache metadata and coverage must match the approved source', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  await writeFile(
    path.join(f.directory, `${ids.station}.metadata.json`),
    JSON.stringify({...f.metadata, note: 'Changed after review'}),
  )
  await assert.rejects(
    applyWillieReplay(
      {
        target: 'prod',
        plan,
        expectedPlanHash: plan.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    ),
    /WILLIE_REPLAY_CACHE_CHANGED/,
  )
  await assert.rejects(
    prepareWillieReplay(
      {...f.options, window: {start: '2025-12-01', end: '2026-01-02'}},
      f.client,
      now,
    ),
    /WILLIE_REPLAY_CACHE_WINDOW_MISMATCH/,
  )
  assert.equal(f.sent.length, 0)
})

void test('Willie refuses absent or ambiguous connectors before ingesting', async (t) => {
  const f = await fixture(t)
  f.point.connector = 'eveler'
  await assert.rejects(
    prepareWillieReplay(f.options, f.client, now),
    /WILLIE_REPLAY_CONNECTOR_INVALID/,
  )
  f.point.connector = 'willie'
  const duplicateClient = {
    ...f.client,
    getContextsForDeclarant: () =>
      Promise.resolve([{contextId: 'a', points: [f.point, f.point]}]),
  }
  await assert.rejects(
    prepareWillieReplay(f.options, duplicateClient, now),
    /WILLIE_REPLAY_CONNECTOR_AMBIGUOUS/,
  )
  assert.equal(f.sent.length, 0)
})

void test('Willie null source is not converted to zero and cannot produce an apply', async (t) => {
  const f = await fixture(
    t,
    response([{dateTime: '2026-01-02', consumption: null}]),
  )
  const plan = await prepareWillieReplay(f.options, f.client, now)
  assert.equal(plan.summary.numericDays, 0)
  assert.equal(plan.summary.nullDays, 1)
  await assert.rejects(
    applyWillieReplay(
      {
        target: 'prod',
        plan,
        expectedPlanHash: plan.planHash,
        cacheDirectory: f.directory,
      },
      f.client,
      now,
    ),
    /WILLIE_REPLAY_NO_NUMERIC_VALUES/,
  )
  assert.equal(f.sent.length, 0)
})

void test('Willie acknowledgement must account for every imported or skipped value', async (t) => {
  const f = await fixture(t)
  const plan = await prepareWillieReplay(f.options, f.client, now)
  for (const acknowledgement of [
    undefined,
    {imported: false, skippedValues: 0},
    {imported: true, skippedValues: 2},
    {imported: true, skippedValues: -1},
    {imported: true, skippedValues: 3},
  ]) {
    f.state.ack = acknowledgement
    await assert.rejects(
      applyWillieReplay(
        {
          target: 'prod',
          plan,
          expectedPlanHash: plan.planHash,
          cacheDirectory: f.directory,
        },
        f.client,
        now,
      ),
      /WILLIE_REPLAY_ACKNOWLEDGEMENT_INVALID/,
    )
  }
})
