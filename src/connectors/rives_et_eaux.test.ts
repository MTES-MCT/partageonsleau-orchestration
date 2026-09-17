import assert from 'node:assert/strict'
import test from 'node:test'
import {
  normalizeRivesRows,
  parseRivesLocalDate,
  rivesDayWindows,
  rivesFingerprint,
  RivesEtEauxConnector,
} from './rives_et_eaux.js'

void test('Rives preserves original local time and exact seconds in Europe/Paris', () => {
  assert.deepEqual(parseRivesLocalDate('2026-09-01T00:11:43'), {
    temporalStatus: 'VALID',
    normalizedObservedAt: '2026-08-31T22:11:43.000Z',
  })
  assert.deepEqual(parseRivesLocalDate('2026-01-01 00:11:43.125'), {
    temporalStatus: 'VALID',
    normalizedObservedAt: '2025-12-31T23:11:43.125Z',
  })
  for (const value of [
    '2026-02-30T12:00:00',
    '2026-03-29T02:30:00',
    '2026-09-01T00:11:43Z',
    '',
    null,
  ]) {
    assert.deepEqual(parseRivesLocalDate(value), {
      temporalStatus: 'INVALID_LOCAL_TIME',
    })
  }

  assert.deepEqual(parseRivesLocalDate('2026-10-25T02:30:00'), {
    temporalStatus: 'AMBIGUOUS_LOCAL_TIME',
  })
})

void test('Rives daily windows use completed local days and follow DST changes', () => {
  const windows = rivesDayWindows(new Date('2026-03-30T02:00:00Z'))
  assert.equal(windows.length, 15)
  assert.equal(windows[0].localStart, '2026-03-15')
  assert.deepEqual(windows.at(-1), {
    localStart: '2026-03-29',
    localEnd: '2026-03-30',
    windowStart: '2026-03-28T23:00:00.000Z',
    windowEnd: '2026-03-29T22:00:00.000Z',
  })
  assert.equal(
    rivesDayWindows(new Date('2026-10-26T04:00:00Z')).at(-1)?.windowStart,
    '2026-10-24T22:00:00.000Z',
  )
  assert.equal(
    rivesDayWindows(new Date('2026-09-16T22:30:00Z')).at(-1)?.localEnd,
    '2026-09-17',
  )
})

void test('Rives deduplicates strict repeats across inclusive windows, preserves conflicts and invalid qualities', () => {
  const row = {
    NumeroSerieCompteur: '000123',
    Date: '2026-09-01T00:11:43',
    Index: 123.4,
    Origine: 'Auto',
    CodeValidite: 'A',
  }
  const rows = [
    row,
    {...row},
    {...row, Index: 124},
    {...row, CodeValidite: 'Y'},
    {...row, Date: 'bad'},
    null,
    'invalid',
  ]
  const seen = new Set<string>()
  const normalized = normalizeRivesRows(rows, seen)
  assert.equal(normalized.length, 6)
  assert.deepEqual(normalized[0], {
    externalId: '000123',
    observedAt: '2026-08-31T22:11:43.000Z',
    index: '123.4',
    status: 'VALID',
    reason: null,
    quality: 'A',
    origin: 'Auto',
    raw: row,
  })
  assert.equal(normalized[1].index, '124')
  assert.equal(normalized[2].status, 'INVALID')
  assert.equal(normalized[2].quality, 'Y')
  assert.equal(normalized[2].reason, 'EXCLUDED_QUALITY')
  assert.equal(normalized[3].observedAt, null)
  assert.equal(normalized[3].reason, 'INVALID_LOCAL_TIME')
  assert.deepEqual(
    normalized.slice(-2).map((reading) => reading.raw),
    [null, 'invalid'],
  )
  assert.ok(
    normalized
      .slice(-2)
      .every(
        (reading) =>
          reading.status === 'INVALID' && reading.reason === 'INVALID_ROW',
      ),
  )
  assert.deepEqual(normalizeRivesRows([row], seen), [])
  assert.equal(normalizeRivesRows([{...row, Date: 'bad'}], seen).length, 1)
  assert.equal(rivesFingerprint({b: 2, a: 1}), rivesFingerprint({a: 1, b: 2}))
})

void test('all provider quality rules live in the Rives connector, not the API contract', () => {
  const row = {
    NumeroSerieCompteur: '00001',
    Date: '2026-09-01T00:11:43',
    Index: '000123.4000',
    Origine: 'Manuel',
  }
  for (const quality of ['A', 'B', 'C', 'D', 'E', 'X']) {
    const [reading] = normalizeRivesRows([{...row, CodeValidite: quality}])
    assert.equal(reading.status, 'VALID')
    assert.equal(reading.externalId, '00001')
    assert.equal(reading.index, '123.4')
    assert.equal(reading.quality, quality)
    assert.equal(reading.origin, 'Manuel')
    assert.equal('NumeroSerieCompteur' in reading, false)
    assert.equal('Date' in reading, false)
  }

  for (const quality of ['Y', 'W', 'Z', '?', null, '']) {
    const [reading] = normalizeRivesRows([{...row, CodeValidite: quality}])
    assert.equal(reading.status, 'INVALID')
    assert.equal(reading.observedAt, '2026-08-31T22:11:43.000Z')
    assert.equal(reading.index, '123.4')
  }
})

void test('malformed values and ambiguous local timestamps retain raw evidence without inventing a reading', () => {
  const row = {
    NumeroSerieCompteur: 'meter',
    Date: '2026-09-01T00:11:43',
    Index: '10',
    CodeValidite: 'A',
  }
  for (const index of [
    -1,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
    '1e3',
    '1,2,5',
    '1.00001',
    '10000000000000000',
    null,
    false,
    '',
  ]) {
    const [reading] = normalizeRivesRows([{...row, Index: index}])
    assert.equal(reading.status, 'INVALID')
    assert.equal(reading.reason, 'INVALID_INDEX')
    assert.equal(reading.index, null)
  }

  assert.equal(
    normalizeRivesRows([{...row, Index: '9999999999999999.9999'}])[0].index,
    '9999999999999999.9999',
  )
  const ambiguous = {...row, Date: '2026-10-25T02:30:00'}
  const [reading] = normalizeRivesRows([ambiguous])
  assert.equal(reading.externalId, 'meter')
  assert.equal(reading.observedAt, null)
  assert.equal(reading.reason, 'AMBIGUOUS_LOCAL_TIME')
  assert.equal(reading.status, 'INVALID')
  assert.deepEqual(reading.raw, ambiguous)
  for (const externalId of [
    null,
    -1,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
    '',
    ' '.repeat(10),
    'a'.repeat(251),
  ]) {
    assert.equal(
      normalizeRivesRows([{...row, NumeroSerieCompteur: externalId}])[0].reason,
      'INVALID_EXTERNAL_ID',
    )
  }
})

void test('Rives preserves numeric serials, comma decimal inputs and case-insensitive quality semantics', () => {
  const raw = {
    NumeroSerieCompteur: 123,
    Date: '2026-09-01T00:11:43',
    Index: ' 001,2500 ',
    CodeValidite: ' a ',
    Origine: 'Auto',
  }
  const [reading] = normalizeRivesRows([raw])
  assert.equal(reading.externalId, '123')
  assert.equal(reading.index, '1.25')
  assert.equal(reading.quality, 'A')
  assert.equal(reading.status, 'VALID')
  assert.deepEqual(reading.raw, raw)
  assert.equal(
    normalizeRivesRows([{...raw, CodeValidite: ' y '}])[0].status,
    'INVALID',
  )
})

void test('Rives fetches all meters once per local window with native fetch and no credential in URL', async () => {
  const calls: Array<{url: string; options: RequestInit | undefined}> = []
  const fetcher: typeof fetch = async (input, options) => {
    calls.push({
      url: input instanceof Request ? input.url : input.toString(),
      options,
    })
    return Response.json([
      {NumeroSerieCompteur: 'first'},
      {NumeroSerieCompteur: 'second'},
    ])
  }

  const connector = new RivesEtEauxConnector(
    'test-secret',
    'https://provider.invalid',
    fetcher,
  )
  const rows = await connector.fetchWindow(
    rivesDayWindows(new Date('2026-09-17T10:00:00Z'))[0],
  )
  assert.equal(rows.length, 2)
  assert.equal(calls.length, 1)
  assert.equal(
    calls[0].url,
    'https://provider.invalid/api/public/Calypso/Export?dateDebut=2026-09-02&dateFin=2026-09-03',
  )
  assert.deepEqual(calls[0].options?.headers, {
    Accept: 'application/json',
    'X-API-Key': 'test-secret',
  })
  assert.equal(calls[0].options?.redirect, 'error')
  assert.ok(calls[0].options?.signal)
  assert.equal('dispatcher' in (calls[0].options ?? {}), false)
})

void test('Rives provider failures cannot expose keys or untrusted response bodies', async () => {
  const window = rivesDayWindows(new Date('2026-09-17T10:00:00Z'))[0]
  for (const status of [401, 403, 429, 500]) {
    const connector = new RivesEtEauxConnector(
      'test-secret',
      'https://provider.invalid',
      async () => new Response('test-secret', {status}),
    )
    await assert.rejects(
      connector.fetchWindow(window),
      (error: Error) =>
        error.message.includes(`HTTP ${status}`) &&
        !error.message.includes('test-secret'),
    )
  }

  for (const body of ['{"items":[]}', 'invalid']) {
    const connector = new RivesEtEauxConnector(
      'test-secret',
      'https://provider.invalid',
      async () => new Response(body),
    )
    await assert.rejects(connector.fetchWindow(window), /JSON/v)
  }

  const connector = new RivesEtEauxConnector(
    'test-secret',
    'https://provider.invalid',
    async () => {
      throw new Error('test-secret')
    },
  )
  await assert.rejects(
    connector.fetchWindow(window),
    (error: Error) => !error.message.includes('test-secret'),
  )
  assert.throws(() => new RivesEtEauxConnector(''), /Missing/v)
  assert.throws(
    () => new RivesEtEauxConnector('test', 'http://provider.invalid'),
    /HTTPS/v,
  )
})
