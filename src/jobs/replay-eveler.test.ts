import assert from 'node:assert/strict'
import test, {type TestContext} from 'node:test'
import {EvelerClient, EvelerConnector} from '../connectors/eveler.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'
import {replayEveler} from './replay-eveler.js'
import {processPoint} from './pull_updated_data.js'

const window = {
  start: new Date('2026-01-01T00:00:00Z'),
  end: new Date('2026-01-01T01:00:00Z'),
}
const point = {
  pointId: 'synthetic-point',
  sourcePointId: 'synthetic-meter',
  exploitationId: 'synthetic-exploitation',
  countingCode: '001',
  connector: 'eveler',
  connectorId: 'synthetic-connector',
  connectorRate: 100,
  connectorParameters: {
    sourceStartDate: window.start.toISOString(),
    sourceMeterId: '000000000000000000000001',
  },
  mostRecentAvailableDate: undefined,
}
function configure(t: TestContext) {
  const settings = {
    PLE_BASE_URL: 'https://ple.invalid',
    CLIENT_ID: 'synthetic-client',
    CLIENT_SECRET: 'synthetic-secret',
    EVELER_API_IDENTIFIER: 'synthetic-provider',
    EVELER_API_SECRET: 'synthetic-secret',
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
}
function providerResponse() {
  return {
    success: true,
    data: {
      meter_id: point.connectorParameters.sourceMeterId,
      start_date: '2026-01-01',
      end_date: '2026-01-02',
      unit: 'm3',
      timestep: 600,
      values: Array.from({length: 6}, (_, index) => ({
        date: new Date(
          window.start.getTime() + (index + 1) * 600_000,
        ).toISOString(),
        value: 0.1,
      })),
    },
  }
}

void test('Eveler replay targets one connector, defaults to dry-run, preserves periods and acknowledges idempotent skips', async (t) => {
  configure(t)
  const posted: Array<Record<string, unknown>> = []
  t.mock.method(globalThis, 'fetch', (input: unknown, init?: RequestInit) => {
    const url = String(input)
    if (url.includes('/auth/login'))
      return Promise.resolve(
        Response.json({
          success: true,
          data: {token: 'synthetic-provider-token'},
        }),
      )
    if (url.includes('/data/volume/'))
      return Promise.resolve(Response.json(providerResponse()))
    if (url.endsWith('/service-accounts/token'))
      return Promise.resolve(Response.json({accessToken: 'synthetic-sa-token'}))
    if (url.endsWith('/context'))
      return Promise.resolve(
        Response.json({
          data: [
            {
              contextId: 'synthetic-context',
              points: [
                point,
                {
                  ...point,
                  connectorId: 'unrelated-connector',
                  connector: 'willie',
                },
              ],
            },
          ],
        }),
      )
    if (url.endsWith('/connectors/ingest')) {
      posted.push(JSON.parse(init?.body as string) as Record<string, unknown>)
      assert.equal(init?.redirect, 'error')
      assert.ok(init.signal)
      return Promise.resolve(
        Response.json(
          posted.length === 1
            ? {
                success: true,
                imported: true,
                sourceId: 'synthetic-source',
                minDate: window.start.toISOString(),
                maxDate: window.end.toISOString(),
                skippedValues: 0,
              }
            : {
                success: true,
                imported: false,
                reason: 'ALL_METRICS_SKIPPED_BY_CONFLICT',
                skippedValues: 1,
              },
        ),
      )
    }
    throw new Error('Unexpected endpoint')
  })
  const options = {
    connectorId: point.connectorId,
    declarantId: 'synthetic-declarant',
    window,
  }
  const provider = new EvelerClient({wait: () => Promise.resolve()})
  const dryRun = await replayEveler(options, undefined, provider)
  assert.equal(dryRun.mode, 'dry-run')
  assert.equal(dryRun.completeHours, 1)
  assert.equal(posted.length, 0)
  assert.equal(
    (await replayEveler({...options, apply: true}, undefined, provider))
      .importedBatches,
    1,
  )
  const repeated = await replayEveler(
    {...options, apply: true},
    undefined,
    provider,
  )
  assert.equal(repeated.importedBatches, 0)
  assert.equal(repeated.skippedValues, 1)
  const payload = posted[0] as {
    connectorId: string
    data: {
      metrics: Array<{
        values: Array<{periodStart: string; periodEnd: string; value: number}>
      }>
    }
  }
  assert.equal(payload.connectorId, point.connectorId)
  assert.deepEqual(payload.data.metrics[0].values[0], {
    date: window.start.toISOString(),
    periodStart: window.start.toISOString(),
    periodEnd: window.end.toISOString(),
    value: 0.6,
  })
  await assert.rejects(
    replayEveler({...options, connectorId: 'unknown-connector'}),
    /exactly one/,
  )
})

void test('Eveler refuses false, partial and missing ingestion acknowledgements and redacts HTTP errors', async (t) => {
  configure(t)
  // Obtain one complete normalized batch using only synthetic provider responses.
  let ingestionResponse: unknown = null
  let ingestionStatus = 200
  t.mock.method(globalThis, 'fetch', (input: unknown) => {
    const url = String(input)
    if (url.includes('/login'))
      return Promise.resolve(
        Response.json({
          success: true,
          data: {token: 'synthetic-provider-token'},
        }),
      )
    if (url.includes('/data/volume/'))
      return Promise.resolve(Response.json(providerResponse()))
    return Promise.resolve(
      ingestionStatus === 200
        ? Response.json(ingestionResponse)
        : new Response('private-remote-body', {status: ingestionStatus}),
    )
  })
  const batch = await new EvelerConnector({
    window,
    client: new EvelerClient({wait: () => Promise.resolve()}),
  }).run({
    ...point,
    serviceAccount: 'synthetic-client',
    rate: 100,
  })
  const client = new PartageonsLeauClient()
  const ingest = () =>
    client.ingest({
      output: batch,
      pointId: point.pointId,
      declarantId: 'synthetic-declarant',
      contextId: 'synthetic-context',
      serviceAccountToken: 'synthetic-token',
      requireAcknowledgement: true,
    })
  for (const response of [
    {success: false},
    {success: true},
    {success: true, imported: true, skippedValues: 0},
    {success: true, imported: false, reason: 'NO_VALUES', skippedValues: 0},
    {
      success: true,
      imported: false,
      reason: 'ALL_METRICS_SKIPPED_BY_CONFLICT',
      skippedValues: 0,
    },
  ]) {
    ingestionResponse = response
    await assert.rejects(ingest(), /acknowledge/)
  }
  ingestionStatus = 503
  await assert.rejects(ingest(), (error: Error) => {
    assert.match(error.message, /503/)
    assert.doesNotMatch(error.message, /private-remote-body/)
    return true
  })
})

void test('PLE context propagates optional connectorParameters in current and legacy formats', async (t) => {
  configure(t)
  let contextResponse: unknown
  t.mock.method(globalThis, 'fetch', () =>
    Promise.resolve(Response.json(contextResponse)),
  )
  for (const response of [
    {data: [{contextId: 'synthetic-context', points: [point]}]},
    {
      success: true,
      exploitations: [
        {
          id: point.exploitationId,
          countingCode: point.countingCode,
          point: {id: point.pointId},
          connectors: [
            {
              id: point.connectorId,
              type: 'eveler',
              parameters: {
                ...point.connectorParameters,
                sourcePointId: point.sourcePointId,
              },
            },
          ],
        },
      ],
    },
  ]) {
    contextResponse = response
    const contexts = await new PartageonsLeauClient().getContextsForDeclarant(
      'synthetic-declarant',
      'synthetic-token',
    )
    assert.equal(
      contexts[0].points[0].connectorParameters?.sourceStartDate,
      point.connectorParameters.sourceStartDate,
    )
  }
})

void test('daily pull propagates parameters, requires acknowledgement and surfaces Eveler failures', async (t) => {
  configure(t)
  const client = new PartageonsLeauClient()
  t.mock.method(globalThis, 'fetch', (input: unknown) =>
    Promise.resolve(
      String(input).includes('/login')
        ? Response.json({
            success: true,
            data: {token: 'synthetic-provider-token'},
          })
        : Response.json(providerResponse()),
    ),
  )
  let ingestions = 0
  const connector = new EvelerConnector({
    window,
    client: new EvelerClient({wait: () => Promise.resolve()}),
  })
  t.mock.method(
    client,
    'ingest',
    (parameters: Parameters<PartageonsLeauClient['ingest']>[0]) => {
      assert.equal(parameters.requireAcknowledgement, true)
      assert.equal(parameters.output.data.metrics[0].values.length, 1)
      ingestions++
      throw new Error('synthetic ingestion failure')
    },
  )
  const parameters = {
    connectorRegistry: new Map([['eveler', connector]]),
    partageonsLeauClient: client,
    serviceAccount: 'synthetic-client',
    serviceAccountToken: 'synthetic-token',
    declarantId: 'synthetic-declarant',
    contextId: 'synthetic-context',
    point,
  }
  await assert.rejects(processPoint(parameters), /synthetic ingestion failure/)
  assert.equal(ingestions, 1)
  delete process.env.EVELER_API_SECRET
  await processPoint(parameters)
  assert.equal(ingestions, 1)
})
