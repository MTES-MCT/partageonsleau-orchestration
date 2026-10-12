import assert from 'node:assert/strict'
import test from 'node:test'
import {WillieConnector} from './willie.js'
import {ConflictPolicy} from './types.js'
import {processPoint} from '../jobs/pull_updated_data.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

const fixedClock = {now: () => new Date('2026-11-01T03:00:00Z')}

void test('Willie conserve zéro, écarte les absences et complète les valeurs manquantes lors du rejeu', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  const cursor = new Date('2026-05-01T00:00:00Z')
  t.mock.method(globalThis, 'fetch', (input: unknown) => {
    const url = new URL(String(input))
    assert.equal(url.searchParams.get('startDate'), cursor.toISOString())
    assert.equal(url.searchParams.get('stationIds'), 'synthetic-station')
    return Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: [
              {dateTime: '2026-05-01T00:00:00Z', consumption: null},
              {dateTime: '2026-05-02T00:00:00Z', consumption: 0},
              {dateTime: '2026-05-03T00:00:00Z', consumption: 12},
            ],
          },
        ],
        count: 3,
        unknownStationIds: [],
      }),
    )
  })
  const result = await new WillieConnector(fixedClock).run({
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: cursor,
  })
  assert.deepEqual(
    result.data.metrics[0].values.map((row) => row.value),
    [0, 12],
  )
  assert.equal(
    result.data.metrics[0].conflictPolicy,
    ConflictPolicy.SKIP_CONFLICTING_VALUES,
  )
})

void test('le pull Willie distingue un lot vide, un rejeu acquitté et un échec à retenter', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  let consumption: number | null = null
  t.mock.method(globalThis, 'fetch', () =>
    Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: [{dateTime: '2026-05-01T00:00:00Z', consumption}],
          },
        ],
        count: 1,
        unknownStationIds: [],
      }),
    ),
  )
  const client = new PartageonsLeauClient()
  let ingestions = 0
  let fail = false
  t.mock.method(
    client,
    'ingest',
    (parameters: Parameters<PartageonsLeauClient['ingest']>[0]) => {
      ingestions++
      assert.equal(parameters.requireAcknowledgement, true)
      if (fail) throw new Error('synthetic ingestion failure')
      return Promise.resolve({imported: false, skippedValues: 1})
    },
  )
  const parameters = {
    connectorRegistry: new Map([['willie', new WillieConnector(fixedClock)]]),
    partageonsLeauClient: client,
    serviceAccount: 'synthetic',
    serviceAccountToken: 'synthetic',
    declarantId: 'synthetic',
    contextId: 'synthetic',
    point: {
      pointId: 'synthetic-point',
      connector: 'willie',
      connectorRate: 100,
      sourcePointId: 'synthetic-station',
      mostRecentAvailableDate: undefined,
    },
  }
  await processPoint(parameters)
  assert.equal(ingestions, 0)
  consumption = 0
  await processPoint(parameters)
  assert.equal(ingestions, 1)
  fail = true
  await assert.rejects(processPoint(parameters), /synthetic ingestion failure/)
})

void test('Willie explicite les journées UTC historiques, y compris aux changements d’heure', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  t.mock.method(globalThis, 'fetch', () =>
    Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: [
              {dateTime: '2026-03-29', consumption: 23},
              {dateTime: '2026-10-25', consumption: 25},
            ],
          },
        ],
        count: 2,
        unknownStationIds: [],
      }),
    ),
  )
  const result = await new WillieConnector(fixedClock).run({
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: undefined,
  })
  const values = result.data.metrics[0].values
  assert.deepEqual(
    values.map((value) => [
      value.periodStart?.toISOString(),
      value.periodEnd?.toISOString(),
    ]),
    [
      ['2026-03-29T00:00:00.000Z', '2026-03-30T00:00:00.000Z'],
      ['2026-10-25T00:00:00.000Z', '2026-10-26T00:00:00.000Z'],
    ],
  )
  assert.equal(result.data.min_date?.toISOString(), '2026-03-29T00:00:00.000Z')
  assert.equal(result.data.max_date?.toISOString(), '2026-10-26T00:00:00.000Z')
})

void test('le vrai client valide les bornes Willie importées, les jours partiellement ignorés et le rejeu complet', async (t) => {
  const settings = {
    WILLIE_API_TOKEN: 'synthetic',
    PLE_BASE_URL: 'https://api.invalid',
    CLIENT_ID: 'synthetic',
    CLIENT_SECRET: 'synthetic',
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
  let skippedValues = 0
  let invalid = false
  t.mock.method(globalThis, 'fetch', (input: unknown, init?: RequestInit) => {
    const url = new URL(String(input))
    if (url.hostname === 'api.meetwillie.com')
      return Promise.resolve(
        Response.json({
          stations: [
            {
              stationID: 'synthetic-station',
              datapoints: [
                {dateTime: '2026-03-28', consumption: 10},
                {dateTime: '2026-03-29', consumption: 20},
                {dateTime: '2026-03-30', consumption: 30},
              ],
            },
          ],
          count: 3,
          unknownStationIds: [],
        }),
      )
    assert.equal(url.pathname, '/service-accounts/connectors/ingest')
    assert.ok(typeof init?.body === 'string')
    const payload = JSON.parse(init.body) as {
      data: {
        min_date: string
        max_date: string
        metrics: Array<{
          values: Array<{periodStart: string; periodEnd: string}>
        }>
      }
    }
    const values = payload.data.metrics[0].values
    assert.equal(values.length, 3)
    assert.equal(values[0].periodStart, '2026-03-28T00:00:00.000Z')
    assert.equal(values[2].periodEnd, '2026-03-31T00:00:00.000Z')
    if (skippedValues === values.length)
      return Promise.resolve(
        Response.json({
          success: true,
          imported: false,
          reason: 'ALL_METRICS_SKIPPED_BY_CONFLICT',
          skippedValues,
        }),
      )
    return Promise.resolve(
      Response.json({
        success: true,
        imported: true,
        sourceId: 'synthetic-source',
        skippedValues,
        minDate: values[skippedValues].periodStart,
        maxDate: invalid ? '2026-04-01T00:00:00.000Z' : values[2].periodEnd,
      }),
    )
  })
  const output = await new WillieConnector(fixedClock).run({
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: undefined,
  })
  const client = new PartageonsLeauClient()
  const parameters = {
    output,
    pointId: 'synthetic-point',
    declarantId: 'synthetic-owner',
    contextId: 'synthetic-context',
    serviceAccountToken: 'synthetic-token',
    requireAcknowledgement: true,
  }
  assert.deepEqual(await client.ingest(parameters), {
    imported: true,
    skippedValues: 0,
  })
  skippedValues = 1
  assert.deepEqual(await client.ingest(parameters), {
    imported: true,
    skippedValues: 1,
  })
  skippedValues = 3
  assert.deepEqual(await client.ingest(parameters), {
    imported: false,
    skippedValues: 3,
  })
  skippedValues = 0
  invalid = true
  await assert.rejects(
    client.ingest(parameters),
    /Incomplete ingestion acknowledgement/,
  )
})

void test('Willie ne transmet que les jours terminés à Paris, à minuit et aux changements d’heure', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  let instant = ''
  let days: string[] = []
  t.mock.method(globalThis, 'fetch', (input: unknown) => {
    assert.equal(
      new URL(String(input)).searchParams.get('endDate'),
      new Date(instant).toISOString(),
    )
    return Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: days.map((dateTime, index) => ({
              dateTime,
              consumption: index === 0 ? 0 : 95,
            })),
          },
        ],
        count: days.length,
        unknownStationIds: [],
      }),
    )
  })
  for (const example of [
    {
      instant: '2026-01-02T22:59:59.999Z',
      days: ['2026-01-01', '2026-01-02', '2026-01-03'],
      accepted: 1,
    },
    {
      instant: '2026-01-02T23:00:00.000Z',
      days: ['2026-01-01', '2026-01-02', '2026-01-03'],
      accepted: 2,
    },
    {
      instant: '2026-03-29T21:59:59.999Z',
      days: ['2026-03-28', '2026-03-29', '2026-03-30'],
      accepted: 1,
    },
    {
      instant: '2026-03-29T22:00:00.000Z',
      days: ['2026-03-28', '2026-03-29', '2026-03-30'],
      accepted: 2,
    },
    {
      instant: '2026-10-25T22:59:59.999Z',
      days: ['2026-10-24', '2026-10-25', '2026-10-26'],
      accepted: 1,
    },
    {
      instant: '2026-10-25T23:00:00.000Z',
      days: ['2026-10-24', '2026-10-25', '2026-10-26'],
      accepted: 2,
    },
  ]) {
    instant = example.instant
    days = example.days
    const result = await new WillieConnector({
      now: () => new Date(instant),
    }).run({
      serviceAccount: 'synthetic',
      sourcePointId: 'synthetic-station',
      rate: 100,
      mostRecentAvailableDate: undefined,
    })
    const values = result.data.metrics[0].values
    assert.deepEqual(
      values.map((value) => value.periodStart?.toISOString()),
      days.slice(0, example.accepted).map((day) => `${day}T00:00:00.000Z`),
      instant,
    )
    assert.equal(values[0].value, 0)
    for (const value of values)
      assert.equal(
        (value.periodEnd!.getTime() - value.periodStart!.getTime()) / 3_600_000,
        24,
      )
  }
})

void test('une réponse Willie traversant minuit garde la borne de départ du HTTP', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  let now = new Date('2026-07-01T21:59:59Z')
  let calls = 0
  t.mock.method(globalThis, 'fetch', () => {
    calls++
    now = new Date('2026-07-01T22:01:00Z')
    return Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: [
              {dateTime: '2026-06-30', consumption: 0},
              {dateTime: '2026-07-01', consumption: calls === 1 ? 95 : 943},
              {dateTime: '2026-07-02', consumption: 12},
            ],
          },
        ],
        count: 3,
        unknownStationIds: [],
      }),
    )
  })
  const connector = new WillieConnector({now: () => new Date(now)})
  const context = {
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: undefined,
  }
  const before = await connector.run(context)
  assert.deepEqual(
    before.data.metrics[0].values.map((value) => value.value),
    [0],
  )
  const after = await connector.run(context)
  assert.deepEqual(
    after.data.metrics[0].values.map((value) => value.value),
    [0, 943],
  )
})

void test('un ancien curseur anticipé relit le dernier jour clos et ne perd pas le lendemain', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  let now = new Date('2026-10-12T03:00:00Z')
  let start = '2026-10-11'
  t.mock.method(globalThis, 'fetch', (input: unknown) => {
    const url = new URL(String(input))
    assert.equal(url.searchParams.get('startDate'), `${start}T00:00:00.000Z`)
    assert.ok(
      Date.parse(url.searchParams.get('startDate')!) <
        Date.parse(url.searchParams.get('endDate')!),
    )
    return Promise.resolve(
      Response.json({
        stations: [
          {
            stationID: 'synthetic-station',
            datapoints: [
              {dateTime: start, consumption: 943},
              {dateTime: now.toISOString().slice(0, 10), consumption: 95},
              {dateTime: '2026-10-14', consumption: 0},
            ],
          },
        ],
        count: 3,
        unknownStationIds: [],
      }),
    )
  })
  const connector = new WillieConnector({now: () => new Date(now)})
  const context = {
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: new Date('2026-10-13T00:00:00Z'),
  }
  const before = await connector.run(context)
  assert.equal(
    before.data.metrics[0].values[0].periodStart?.toISOString(),
    '2026-10-11T00:00:00.000Z',
  )
  assert.equal(before.data.metrics[0].values.length, 1)
  now = new Date('2026-10-13T03:00:00Z')
  start = '2026-10-12'
  const after = await connector.run(context)
  assert.equal(
    after.data.metrics[0].values[0].periodStart?.toISOString(),
    '2026-10-12T00:00:00.000Z',
  )
  assert.equal(after.data.metrics[0].values.length, 1)
  assert.equal(
    after.data.metrics[0].conflictPolicy,
    ConflictPolicy.SKIP_CONFLICTING_VALUES,
  )
})

void test('Willie ne recule pas son activation si aucun jour activé n’est encore terminé', async (t) => {
  const previous = process.env.WILLIE_API_TOKEN
  process.env.WILLIE_API_TOKEN = 'synthetic'
  t.after(() => {
    if (previous === undefined) delete process.env.WILLIE_API_TOKEN
    else process.env.WILLIE_API_TOKEN = previous
  })
  t.mock.method(globalThis, 'fetch', () => {
    throw new Error('NO_PROVIDER_REQUEST_EXPECTED')
  })
  const result = await new WillieConnector({
    now: () => new Date('2026-01-01T12:00:00Z'),
  }).run({
    serviceAccount: 'synthetic',
    sourcePointId: 'synthetic-station',
    rate: 100,
    mostRecentAvailableDate: undefined,
  })
  assert.deepEqual(result.data.metrics[0].values, [])
})
