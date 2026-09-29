import assert from 'node:assert/strict'
import test from 'node:test'
import {BaseConnector} from '../connectors/base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ParsedPointPayload,
} from '../connectors/types.js'
import {PartageonsLeauClient} from './partageonsleau-client.js'

class SyntheticConnector extends BaseConnector<undefined, undefined> {
  constructor(private readonly countingCode?: string) {
    super('synthetic')
  }

  protected fetch(): Promise<undefined> {
    return Promise.resolve(undefined)
  }
  protected parse(): Promise<undefined> {
    return Promise.resolve(undefined)
  }
  protected process(): Promise<ParsedPointPayload> {
    return Promise.resolve({
      id_point_de_prelevement: 'source-1',
      source_type: SourceType.API,
      source_metadata: undefined,
      min_date: undefined,
      max_date: undefined,
      metrics: [
        {
          type: MetricType.VOLUME,
          countingCode: this.countingCode,
          granularity: Granularity.DAY,
          conflictPolicy: ConflictPolicy.REPLACE_EXISTING,
          unit: MetricUnit.M3,
          values: [{date: new Date('2026-01-02'), value: 12}],
        },
      ],
    })
  }
}

void test('le connecteur refuse une identité de métrique contradictoire', async () => {
  await assert.rejects(
    new SyntheticConnector('002').run({
      serviceAccount: 'synthetic',
      sourcePointId: 'source-1',
      exploitationId: 'exploitation-1',
      countingCode: '001',
      rate: 100,
      mostRecentAvailableDate: undefined,
    }),
    /identité du comptage/,
  )
})

void test('le contexte classique et le transport conservent deux exploitations au même PP', async (t) => {
  const previous = Object.fromEntries(
    ['PLE_BASE_URL', 'CLIENT_ID', 'CLIENT_SECRET'].map((key) => [
      key,
      process.env[key],
    ]),
  )
  Object.assign(process.env, {
    PLE_BASE_URL: 'https://api.invalid',
    CLIENT_ID: 'synthetic',
    CLIENT_SECRET: 'synthetic',
  })
  t.after(() => {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = value
      }
    }
  })
  const posted: Record<string, unknown>[] = []
  t.mock.method(globalThis, 'fetch', (_input: unknown, init?: RequestInit) => {
    if (init?.method === 'POST') {
      assert.equal(typeof init.body, 'string')
      posted.push(JSON.parse(init.body as string) as Record<string, unknown>)
      return Promise.resolve(Response.json({success: true}))
    }
    return Promise.resolve(
      Response.json({
        success: true,
        exploitations: ['001', '002'].map((countingCode) => ({
          id: `exploitation-${countingCode}`,
          countingCode,
          point: {id: 'point-1'},
          connectors: [
            {
              id: `connector-${countingCode}`,
              type: 'synthetic',
              parameters: {sourcePointId: 'source-1'},
            },
          ],
        })),
      }),
    )
  })
  const client = new PartageonsLeauClient()
  const contexts = await client.getContextsForDeclarant(
    'declarant-1',
    'synthetic-token',
  )
  assert.deepEqual(
    contexts[0].points.map((point) => [
      point.exploitationId,
      point.countingCode,
    ]),
    [
      ['exploitation-001', '001'],
      ['exploitation-002', '002'],
    ],
  )
  for (const point of contexts[0].points) {
    const output = await new SyntheticConnector().run({
      ...point,
      serviceAccount: 'synthetic',
      rate: 100,
    })
    await client.ingest({
      output,
      pointId: point.pointId,
      declarantId: 'declarant-1',
      contextId: contexts[0].contextId,
      serviceAccountToken: 'synthetic-token',
    })
  }
  assert.equal(posted.length, 2)
  for (const [index, countingCode] of ['001', '002'].entries()) {
    const payload = posted[index] as {
      exploitationId: string
      metadata: {exploitation_id: string; counting_code: string}
      data: {
        metrics: Array<{
          exploitationId: string
          countingCode: string
          values: Array<{value: number}>
        }>
      }
    }
    assert.equal(payload.exploitationId, `exploitation-${countingCode}`)
    assert.equal(
      payload.metadata.exploitation_id,
      `exploitation-${countingCode}`,
    )
    assert.equal(payload.metadata.counting_code, countingCode)
    assert.equal(
      payload.data.metrics[0].exploitationId,
      `exploitation-${countingCode}`,
    )
    assert.equal(payload.data.metrics[0].countingCode, countingCode)
    assert.equal(payload.data.metrics[0].values[0].value, 12)
  }
})
