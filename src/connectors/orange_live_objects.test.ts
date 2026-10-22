import assert from 'node:assert/strict'
import test from 'node:test'
import {OrangeLiveObjectsConnector} from './orange_live_objects.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  type ConnectorRunContext,
} from './types.js'

const start = new Date('2026-06-01T00:00:00Z')
const now = new Date('2026-07-01T00:00:00Z')
const context: ConnectorRunContext = {
  serviceAccount: 'synthetic-account',
  sourcePointId: 'urn:lo:nsid:imei:synthetic-meter',
  connectorId: 'synthetic-connector',
  rate: 100,
  mostRecentAvailableDate: start,
}
const record = (index: number) => ({
  id: `synthetic-${index}`,
  streamId: context.sourcePointId,
  timestamp: new Date(start.getTime() + index * 3_600_000).toISOString(),
  value: {genericSensor: {'0': {sensorValue: 0}, '1': {sensorValue: index}}},
})
const connector = (fetcher: typeof fetch) =>
  new OrangeLiveObjectsConnector({
    apiKey: 'synthetic-key',
    fetcher,
    now: () => now,
  })
const fromPages = (pages: unknown[]) => {
  let index = 0
  return connector(() => Promise.resolve(Response.json(pages[index++])))
}
const requestUrl = (input: Parameters<typeof fetch>[0]) =>
  input instanceof Request ? input.url : String(input)

void test('Orange paginates newest-first through an empty page and ingests the oldest 500 indexes', async () => {
  const rows = Array.from({length: 503}, (_, index) => record(index)).reverse()
  const pages = [rows.slice(0, 500), rows.slice(500), []]
  const calls: Array<{url: URL; init?: RequestInit}> = []
  const output = await connector((input, init) => {
    calls.push({url: new URL(requestUrl(input)), init})
    return Promise.resolve(Response.json(pages[calls.length - 1]))
  }).run(context)
  assert.equal(calls.length, 3)
  assert.equal(calls[0].url.searchParams.get('bookmarkId'), null)
  assert.equal(calls[1].url.searchParams.get('bookmarkId'), 'synthetic-3')
  assert.equal(calls[2].url.searchParams.get('bookmarkId'), 'synthetic-0')
  for (const call of calls) {
    assert.equal(call.url.origin, 'https://liveobjects.orange-business.com')
    assert.equal(
      decodeURIComponent(call.url.pathname.split('/').at(-1)!),
      context.sourcePointId,
    )
    assert.equal(call.url.searchParams.get('limit'), '500')
    assert.equal(
      call.url.searchParams.get('timeRange'),
      `${start.toISOString()},${now.toISOString()}`,
    )
    assert.equal(call.init?.method, 'GET')
    assert.equal(call.init?.redirect, 'error')
    assert.ok(call.init?.signal instanceof AbortSignal)
    assert.equal(
      new Headers(call.init?.headers).get('X-API-Key'),
      'synthetic-key',
    )
  }
  assert.equal(output.data.metrics[0].values.length, 500)
  assert.equal(output.data.metrics[0].values[0].value, 0)
  assert.equal(output.data.metrics[0].values[499].value, 499)
  assert.equal(output.data.min_date?.toISOString(), record(0).timestamp)
  assert.equal(
    output.data.max_date?.toISOString(),
    new Date(Date.parse(record(499).timestamp) + 900_000).toISOString(),
  )
  assert.equal(output.data.source_metadata?.sensor_index, '1')
  assert.equal(output.data.metrics[0].type, MetricType.INDEX)
  assert.equal(output.data.metrics[0].unit, MetricUnit.M3)
  assert.equal(output.data.metrics[0].granularity, Granularity.FIFTEEN_MINUTES)
  assert.equal(
    output.data.metrics[0].conflictPolicy,
    ConflictPolicy.REPLACE_EXISTING,
  )
  const resumed = await fromPages([rows.slice(0, 3), []]).run({
    ...context,
    mostRecentAvailableDate: new Date(
      Date.parse(record(499).timestamp) + 900_000,
    ),
  })
  assert.deepEqual(
    resumed.data.metrics[0].values.map((value) => value.value),
    [500, 501, 502],
  )
})

void test('Orange never splits the final normalized quarter-hour when limiting a batch', async () => {
  const rows = Array.from({length: 501}, (_, index) => record(index))
  rows[500].timestamp = new Date(
    Date.parse(rows[499].timestamp) + 60_000,
  ).toISOString()
  const output = await fromPages([rows.reverse(), []]).run(context)
  assert.equal(output.data.metrics[0].values.length, 499)
  assert.equal(
    output.data.max_date?.toISOString(),
    new Date(Date.parse(record(498).timestamp) + 900_000).toISOString(),
  )
  const crowded = Array.from({length: 501}, (_, index) => ({
    ...record(index),
    timestamp: new Date(start.getTime() + index * 1000).toISOString(),
  }))
  await assert.rejects(
    fromPages([crowded, []]).run(context),
    /Too many indexes in one 15-minute bucket/,
  )
})

void test('Orange deduplicates retransmissions and overlapping IDs without changing the UTC observation', async () => {
  const first = record(1)
  const retransmission = {
    ...first,
    id: 'synthetic-retransmission',
    timestamp: '2026-06-01T03:00:00+02:00',
  }
  const output = await fromPages([
    [record(2), first],
    [first, retransmission, record(0)],
    [],
  ]).run(context)
  assert.deepEqual(
    output.data.metrics[0].values,
    [0, 1, 2].map((index) => ({
      date: new Date(record(index).timestamp),
      periodStart: new Date(record(index).timestamp),
      periodEnd: new Date(Date.parse(record(index).timestamp) + 900_000),
      value: index,
    })),
  )
})

void test('Orange refuses contradictory observations, changed record IDs and pagination cycles', async () => {
  await assert.rejects(
    fromPages([
      [record(1), {...record(2), timestamp: record(1).timestamp}],
      [],
    ]).run(context),
    /Conflicting indexes at the same timestamp/,
  )
  await assert.rejects(
    fromPages([[record(1)], [{...record(2), id: record(1).id}], []]).run(
      context,
    ),
    /Conflicting Orange record ID/,
  )
  await assert.rejects(
    fromPages([[record(2), record(1)], [record(2), record(1)], []]).run(
      context,
    ),
    /pagination did not advance/,
  )
})

void test('Orange validates stream identity, record IDs, timestamp bounds and finite sensor values', async () => {
  for (const row of [
    {...record(1), streamId: 'another-stream'},
    {...record(1), id: ''},
    {...record(1), timestamp: 'invalid'},
    {...record(1), timestamp: '2026-05-31T23:59:59Z'},
    {...record(1), timestamp: now.toISOString()},
    {...record(1), value: {genericSensor: {'1': {sensorValue: '1'}}}},
    {...record(1), value: {genericSensor: {'1': {sensorValue: null}}}},
    {...record(1), value: []},
  ]) {
    await assert.rejects(fromPages([[row], []]).run(context), /Invalid/)
  }
  // JSON numbers can overflow to Infinity even though NaN/Infinity are not JSON literals.
  await assert.rejects(
    connector(() =>
      Promise.resolve(
        new Response(
          JSON.stringify([record(1)]).replace(
            '"sensorValue":1',
            '"sensorValue":1e999',
          ),
        ),
      ),
    ).run(context),
    /Invalid Orange response/,
  )
})

void test('Orange never turns a missing channel into zero and resumes from the supplied sensor cursor', async () => {
  const output = await fromPages([
    [{...record(0), value: {genericSensor: {'0': {sensorValue: 42}}}}],
    [],
  ]).run(context)
  assert.deepEqual(output.data.metrics[0].values, [])
  assert.equal(output.data.min_date, undefined)
  assert.equal(output.data.max_date, undefined)
  const ranges: string[] = []
  const empty = connector((input) => {
    ranges.push(new URL(requestUrl(input)).searchParams.get('timeRange')!)
    return Promise.resolve(Response.json([]))
  })
  await empty.run({...context, mostRecentAvailableDate: undefined})
  await empty.run(context)
  await empty.run({...context, mostRecentAvailableDate: now})
  assert.deepEqual(ranges, [
    `2026-01-01T00:00:00.000Z,${now.toISOString()}`,
    `${start.toISOString()},${now.toISOString()}`,
  ])
})

void test('Orange request failures are bounded and never expose provider response bodies or fetch errors', async () => {
  for (const status of [401, 429, 503]) {
    let calls = 0
    await assert.rejects(
      connector(() => {
        calls++
        return Promise.resolve(new Response('synthetic-private-body', {status}))
      }).run(context),
      (error: Error) => {
        assert.match(error.message, new RegExp(`HTTP ${status}`))
        assert.doesNotMatch(
          error.message,
          /synthetic-private-body|synthetic-key/,
        )
        return true
      },
    )
    assert.equal(calls, 1)
  }
  await assert.rejects(
    connector(() => {
      throw new Error('synthetic-key in untrusted network error')
    }).run(context),
    /Provider request failed or timed out\.$/,
  )
  await assert.rejects(
    connector(() =>
      Promise.resolve(new Response('synthetic-private-body')),
    ).run(context),
    /Invalid Orange JSON response\.$/,
  )
})
