import assert from 'node:assert/strict'
import test from 'node:test'
import {OrangeLiveObjectsConnector} from '../connectors/orange_live_objects.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'
import {processPoint} from './pull_updated_data.js'

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
