import assert from 'node:assert/strict'
import test from 'node:test'
import {OrangeLiveObjectsConnector} from '../connectors/orange_live_objects.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'
import {processPoint, pullUpdatedData} from './pull_updated_data.js'

void test('Orange daily pull skips empty batches and requires an acknowledgement for each non-empty batch', async (t) => {
  const client = new PartageonsLeauClient()
  let empty = true
  let calls = 0
  const connector = new OrangeLiveObjectsConnector({
    apiKey: 'synthetic-key',
    now: () => new Date('2026-06-02T00:00:00Z'),
    fetcher: (input) =>
      Promise.resolve(
        Response.json(
          empty ||
            new URL(
              input instanceof Request ? input.url : String(input),
            ).searchParams.has('bookmarkId')
            ? []
            : [
                {
                  id: 'synthetic-record',
                  streamId: 'synthetic-stream',
                  timestamp: '2026-06-01T00:00:00Z',
                  value: {genericSensor: {'1': {sensorValue: 42}}},
                },
              ],
        ),
      ),
  })
  t.mock.method(
    client,
    'ingest',
    (parameters: Parameters<PartageonsLeauClient['ingest']>[0]) => {
      calls++
      assert.equal(parameters.requireAcknowledgement, true)
      return Promise.resolve(undefined)
    },
  )
  const parameters = {
    connectorRegistry: new Map([['orange_live_objects', connector]]),
    partageonsLeauClient: client,
    serviceAccount: 'synthetic-account',
    serviceAccountToken: 'synthetic-token',
    declarantId: 'synthetic-declarant',
    contextId: 'synthetic-context',
    point: {
      pointId: 'synthetic-point',
      connector: 'orange_live_objects',
      sourcePointId: 'synthetic-stream',
      connectorRate: 100,
      mostRecentAvailableDate: new Date('2026-06-01T00:00:00Z'),
    },
  }
  await processPoint(parameters)
  assert.equal(calls, 0)
  empty = false
  await assert.rejects(
    processPoint(parameters),
    /Missing orange_live_objects ingestion acknowledgement/,
  )
  assert.equal(calls, 1)
})

void test('Orange ingestion preserves the existing API quarter-hour bounds and accepts its real acknowledgement format', async (t) => {
  const settings = {
    PLE_BASE_URL: 'https://ple.invalid',
    CLIENT_ID: 'synthetic-account',
    CLIENT_SECRET: 'synthetic-secret',
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
  const client = new PartageonsLeauClient()
  const connector = new OrangeLiveObjectsConnector({
    apiKey: 'synthetic-key',
    now: () => new Date('2026-06-02T00:00:00Z'),
    fetcher: (input) =>
      Promise.resolve(
        Response.json(
          new URL(
            input instanceof Request ? input.url : String(input),
          ).searchParams.has('bookmarkId')
            ? []
            : [
                {
                  id: 'latest',
                  streamId: 'synthetic-stream',
                  timestamp: '2026-06-01T07:59:00Z',
                  value: {genericSensor: {'1': {sensorValue: 22}}},
                },
                {
                  id: 'earlier',
                  streamId: 'synthetic-stream',
                  timestamp: '2026-06-01T07:58:41Z',
                  value: {genericSensor: {'1': {sensorValue: 20}}},
                },
              ],
        ),
      ),
  })
  let ingestions = 0
  t.mock.method(
    globalThis,
    'fetch',
    (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      assert.equal(
        input instanceof Request ? input.url : String(input),
        'https://ple.invalid/service-accounts/connectors/ingest',
      )
      assert.ok(typeof init?.body === 'string')
      const payload = JSON.parse(init.body) as {
        data: {
          min_date: string
          max_date: string
          metrics: Array<{
            values: Array<{
              date: string
              periodStart: string
              periodEnd: string
              value: number
            }>
          }>
        }
      }
      assert.equal(payload.data.min_date, '2026-06-01T07:45:00.000Z')
      assert.equal(payload.data.max_date, '2026-06-01T08:00:00.000Z')
      assert.deepEqual(payload.data.metrics[0].values, [
        {
          date: '2026-06-01T07:59:00.000Z',
          periodStart: '2026-06-01T07:45:00.000Z',
          periodEnd: '2026-06-01T08:00:00.000Z',
          value: 22,
        },
      ])
      ingestions++
      return Promise.resolve(
        Response.json({
          success: true,
          imported: true,
          sourceId: 'synthetic-source',
          minDate: '2026-06-01T07:45:00.000Z',
          maxDate: '2026-06-01T08:00:00.000Z',
          skippedValues: 0,
        }),
      )
    },
  )
  await processPoint({
    connectorRegistry: new Map([['orange_live_objects', connector]]),
    partageonsLeauClient: client,
    serviceAccount: 'synthetic-account',
    serviceAccountToken: 'synthetic-token',
    declarantId: 'synthetic-declarant',
    contextId: 'synthetic-context',
    point: {
      pointId: 'synthetic-point',
      connector: 'orange_live_objects',
      sourcePointId: 'synthetic-stream',
      connectorRate: 100,
      mostRecentAvailableDate: new Date('2026-06-01T00:00:00Z'),
    },
  })
  assert.equal(ingestions, 1)
})

void test('Orange failure does not prevent collecting the remaining points before the pull job fails', async (t) => {
  t.mock.method(
    PartageonsLeauClient.prototype,
    'getAvailableServiceAccounts',
    () => Promise.resolve(['synthetic-account']),
  )
  t.mock.method(PartageonsLeauClient.prototype, 'getServiceAccountToken', () =>
    Promise.resolve('synthetic-token'),
  )
  t.mock.method(
    PartageonsLeauClient.prototype,
    'getDeclarantsForServiceAccount',
    () => Promise.resolve([{id: 'synthetic-declarant', name: 'Synthetic'}]),
  )
  t.mock.method(PartageonsLeauClient.prototype, 'getContextsForDeclarant', () =>
    Promise.resolve([
      {
        contextId: 'synthetic-context',
        points: ['failing-stream', 'next-stream'].map((sourcePointId) => ({
          pointId: 'synthetic-point',
          sourcePointId,
          connector: 'orange_live_objects',
          connectorRate: 100,
          mostRecentAvailableDate: undefined,
        })),
      },
    ]),
  )
  const requested: string[] = []
  const connector = new OrangeLiveObjectsConnector({
    apiKey: 'synthetic-key',
    now: () => new Date('2026-06-02'),
    fetcher: (input) => {
      const streamId = new URL(
        input instanceof Request ? input.url : String(input),
      ).pathname
        .split('/')
        .at(-1)!
      requested.push(streamId)
      return Promise.resolve(
        streamId === 'failing-stream'
          ? new Response('', {status: 503})
          : Response.json([]),
      )
    },
  })
  await assert.rejects(
    pullUpdatedData(new Map([['orange_live_objects', connector]])),
    /1 connector\(s\) failed/,
  )
  assert.deepEqual(requested, ['failing-stream', 'next-stream'])
})
