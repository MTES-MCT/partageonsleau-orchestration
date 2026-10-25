import assert from 'node:assert/strict'
import {access, mkdtemp, readdir, rm, writeFile} from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import {
  buildReunionBatchPlan,
  processNativeDeclarationFiles,
} from './process-declaration.js'
import type {NativeSeries} from '../connectors/reunion-types.js'
import {
  REUNION_BATCH_MAX_BYTES,
  REUNION_BATCH_TARGET_BYTES,
} from './reunion-batch-spool.js'

const files = ['b', 'a', 'c'].map((id) => ({
  id,
  sourceFileId: `legacy-${id}`,
  filename: `${id}.xlsx`,
  type: 'data',
  url: `https://storage.invalid/${id}?signature=one`,
}))
const declaration = {
  id: 'declaration',
  type: 'reunion_multi_param',
  declarantUserId: 'owner',
  files,
  points: [],
}

void test('native batch identity is stable across file ordering and signed URL renewal', () => {
  const first = buildReunionBatchPlan(declaration)
  const renewed = buildReunionBatchPlan({
    ...declaration,
    files: [...files]
      .reverse()
      .map((file) => ({...file, url: file.url.replace('one', 'two')})),
  })
  assert.deepEqual(
    first.map((item) => item.batch),
    renewed.map((item) => item.batch),
  )
  assert.deepEqual(
    first.map((item) => item.file?.id),
    ['a', 'b', 'c'],
  )
  assert.deepEqual(
    first.map((item) => [item.batch.index, item.batch.total]),
    [
      [0, 3],
      [1, 3],
      [2, 3],
    ],
  )
  assert.notEqual(
    first[0].batch.id,
    buildReunionBatchPlan({
      ...declaration,
      files: files.map((file) => ({...file, checksum: 'changed'})),
    })[0].batch.id,
  )
})

void test('native files are released one at a time and all parts are prepared before any ingestion', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'reunion-batches-'))
  const events: string[] = []
  const posted: Array<{batch: unknown; fileId: unknown; value: unknown}> = []
  let previousFile: string | undefined
  try {
    const result = await processNativeDeclarationFiles(
      {declaration, token: 'synthetic', directory},
      {
        downloadFile: async ({file, directory}) => {
          if (previousFile)
            await assert.rejects(access(previousFile), {code: 'ENOENT'})
          events.push(`download:${file.id}`)
          const localPath = path.join(directory, file.id)
          await writeFile(localPath, 'synthetic')
          previousFile = localPath
          return {...file, localPath}
        },
        parse: (input) => {
          events.push(`parse:${input.fileId}`)
          const series: NativeSeries = {
            nativeTemporal: true,
            seriesKey: input.fileId!,
            pointPrelevement: 'reunion:DEP-974:point:1',
            parameter: 'autre',
            unit: 'autre',
            frequency: 'instant',
            valueType: 'instantaneous',
            autoCalculateVolumes: false,
            minDate: '2026-01-01',
            maxDate: '2026-01-01',
            metadata: {},
            data: [
              {
                date: '2026-01-01T00:00:00.123Z',
                periodStart: '2026-01-01T00:00:00.123Z',
                periodEnd: '2026-01-01T00:00:00.123Z',
                value: '1.123456789012345',
                metadata: {row: 13, cell: 'C13'},
              },
            ],
          }
          return {series: [series], errors: []}
        },
        ingest: async ({result}) => {
          await assert.rejects(access(previousFile!), {code: 'ENOENT'})
          events.push(`ingest:${result.data?.batch?.index}`)
          posted.push({
            batch: result.data?.batch,
            fileId: (result.data?.series[0] as NativeSeries).seriesKey,
            value: result.data?.series[0].data[0].value,
          })
        },
      },
    )
    assert.deepEqual(result, {seriesCount: 3, errorCount: 0, batchCount: 3})
    assert.deepEqual(events, [
      'download:a',
      'parse:legacy-a',
      'download:b',
      'parse:legacy-b',
      'download:c',
      'parse:legacy-c',
      'ingest:0',
      'ingest:1',
      'ingest:2',
    ])
    assert.deepEqual(
      posted.map((part) => part.fileId),
      ['legacy-a', 'legacy-b', 'legacy-c'],
    )
    assert.ok(posted.every((part) => part.value === '1.123456789012345'))
    await assert.rejects(access(previousFile!), {code: 'ENOENT'})
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

void test('a failed acknowledgment stops subsequent sends and retries keep the same batch identity', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'reunion-batches-'))
  const downloaded: string[] = []
  const batches: string[] = []
  try {
    for (let attempt = 0; attempt < 2; attempt++)
      await assert.rejects(
        processNativeDeclarationFiles(
          {declaration, token: 'synthetic', directory},
          {
            downloadFile: async ({file, directory}) => {
              downloaded.push(file.id)
              const localPath = path.join(directory, file.id)
              await writeFile(localPath, 'synthetic')
              return {...file, localPath}
            },
            parse: () => ({
              series: [],
              errors: [
                {
                  filename: 'a.xlsx',
                  code: 'NO_DATA',
                  severity: 'error',
                  message: 'Vide',
                },
              ],
            }),
            ingest: async ({result}) => {
              batches.push(result.data!.batch!.id)
              await Promise.resolve()
              throw new Error('HTTP 503')
            },
          },
        ),
        /HTTP 503/,
      )
    assert.deepEqual(downloaded, ['a', 'b', 'c', 'a', 'b', 'c'])
    assert.equal(batches[0], batches[1])
    await assert.rejects(access(path.join(directory, 'a')), {code: 'ENOENT'})
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

void test('a declaration without files still sends one explicit final batch', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'reunion-batches-'))
  let received: unknown
  try {
    const result = await processNativeDeclarationFiles(
      {
        declaration: {...declaration, files: []},
        token: 'synthetic',
        directory,
      },
      {
        ingest: async ({result}) => {
          received = result
          await Promise.resolve()
        },
      },
    )
    assert.deepEqual(result, {seriesCount: 0, errorCount: 1, batchCount: 1})
    assert.equal(
      (received as {data: {batch: {total: number}}}).data.batch.total,
      1,
    )
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

function largeSeries(key: string, padding: string): NativeSeries {
  return {
    nativeTemporal: true,
    seriesKey: key,
    pointPrelevement: 'reunion:DEP-974:point:1',
    parameter: 'autre',
    unit: 'autre',
    frequency: 'instant',
    valueType: 'instantaneous',
    autoCalculateVolumes: false,
    minDate: '2026-01-01',
    maxDate: '2026-01-01',
    metadata: {},
    data: [
      {
        date: '2026-01-01T00:00:00.123Z',
        periodStart: '2026-01-01T00:00:00.123Z',
        periodEnd: '2026-01-01T00:00:00.123Z',
        value: '1.12345678901234567890123456789',
        remark: padding,
        metadata: {row: 13, cell: 'C13'},
      },
    ],
  }
}

void test('retry after an accepted part repeats the same receipts; changed content gets a new batch', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'reunion-retry-'))
  const receipts: Array<{id: string; index: number}> = []
  let failOnce = true
  let value = '1'
  const dependencies: NonNullable<
    Parameters<typeof processNativeDeclarationFiles>[1]
  > = {
    downloadFile: async ({file, directory}) => {
      const localPath = path.join(directory, file.id)
      await writeFile(localPath, 'synthetic')
      return {...file, localPath}
    },
    parse: ({fileId}) => {
      const item = largeSeries(fileId!, '')
      item.data[0].value = value
      return {series: [item], errors: []}
    },
    ingest: async ({result}) => {
      const batch = result.data!.batch!
      receipts.push({id: batch.id, index: batch.index})
      await Promise.resolve()
      if (failOnce && batch.index === 1) {
        failOnce = false
        throw new Error('HTTP 503 after first receipt')
      }
    },
  }
  const input = {declaration, token: 'synthetic', directory}
  try {
    await assert.rejects(
      processNativeDeclarationFiles(input, dependencies),
      /HTTP 503/,
    )
    assert.deepEqual(await readdir(directory), [])
    await processNativeDeclarationFiles(input, dependencies)
    assert.deepEqual(
      receipts.map((receipt) => receipt.index),
      [0, 1, 0, 1, 2],
    )
    assert.equal(new Set(receipts.map((receipt) => receipt.id)).size, 1)
    value = '2'
    await processNativeDeclarationFiles(input, dependencies)
    assert.notEqual(receipts[0].id, receipts.at(-1)!.id)
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

void test('a later download failure discards the spool without making any API request', async () => {
  const directory = await mkdtemp(
    path.join(os.tmpdir(), 'reunion-download-failure-'),
  )
  let sends = 0
  try {
    await assert.rejects(
      processNativeDeclarationFiles(
        {declaration, token: 'synthetic', directory},
        {
          downloadFile: async ({file, directory}) => {
            if (file.id === 'b') throw new Error('Storage unavailable')
            const localPath = path.join(directory, file.id)
            await writeFile(localPath, 'synthetic')
            return {...file, localPath}
          },
          parse: ({fileId}) => ({
            series: [largeSeries(fileId!, '')],
            errors: [],
          }),
          ingest: async () => {
            sends++
            await Promise.resolve()
          },
        },
      ),
      /Storage unavailable/,
    )
    assert.equal(sends, 0)
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

void test('a 60 MiB UTF-8 payload sends whole series below 16 MiB without losing errors or exact values', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'reunion-large-'))
  const padding = 'é'.repeat(6 * 1024 * 1024)
  const series = Array.from({length: 5}, (_, index) =>
    largeSeries(`series-${index}`, padding),
  )
  const errors = [
    {
      filename: 'synthetic.xlsx',
      code: 'SYNTHETIC_WARNING',
      severity: 'warning' as const,
      message: 'Écart conservé',
      rawValue: 'é',
    },
  ]
  const keys: string[] = [],
    sizes: number[] = [],
    receivedErrors: unknown[] = []
  const batches: Array<{id: string; index: number; total: number}> = []
  try {
    const result = await processNativeDeclarationFiles(
      {
        declaration: {...declaration, files: [files[0]]},
        token: 'synthetic',
        directory,
      },
      {
        downloadFile: async ({file, directory}) => {
          const localPath = path.join(directory, file.id)
          await writeFile(localPath, 'synthetic')
          return {...file, localPath}
        },
        parse: () => ({series, errors}),
        ingest: async ({result}) => {
          const bytes = Buffer.byteLength(JSON.stringify(result))
          assert.ok(bytes < REUNION_BATCH_TARGET_BYTES)
          sizes.push(bytes)
          batches.push(result.data!.batch!)
          receivedErrors.push(...result.errors)
          for (const item of result.data!.series) {
            keys.push((item as NativeSeries).seriesKey)
            assert.equal(item.data[0].value, '1.12345678901234567890123456789')
            assert.equal((item as NativeSeries).data[0].remark, padding)
          }
          await Promise.resolve()
        },
      },
    )
    assert.deepEqual(result, {seriesCount: 5, errorCount: 1, batchCount: 5})
    assert.ok(sizes.reduce((sum, bytes) => sum + bytes, 0) > 60 * 1024 * 1024)
    assert.deepEqual(
      keys,
      series.map((item) => item.seriesKey),
    )
    assert.deepEqual(receivedErrors, errors)
    assert.deepEqual(
      batches.map((batch) => [batch.index, batch.total]),
      [
        [0, 5],
        [1, 5],
        [2, 5],
        [3, 5],
        [4, 5],
      ],
    )
    assert.equal(new Set(batches.map((batch) => batch.id)).size, 1)
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})

void test('a single series over 16 MiB is dedicated; over 48 MiB aborts before every API write', async () => {
  const directory = await mkdtemp(
    path.join(os.tmpdir(), 'reunion-large-series-'),
  )
  let padding = 'x'.repeat(17 * 1024 * 1024)
  let sends = 0
  const dependencies: NonNullable<
    Parameters<typeof processNativeDeclarationFiles>[1]
  > = {
    downloadFile: async ({file, directory}) => {
      const localPath = path.join(directory, file.id)
      await writeFile(localPath, 'synthetic')
      return {...file, localPath}
    },
    parse: () => ({series: [largeSeries('single', padding)], errors: []}),
    ingest: async ({result}: {result: unknown}) => {
      sends++
      const bytes = Buffer.byteLength(JSON.stringify(result))
      assert.ok(
        bytes > REUNION_BATCH_TARGET_BYTES && bytes < REUNION_BATCH_MAX_BYTES,
      )
      await Promise.resolve()
    },
  }
  try {
    const input = {
      declaration: {...declaration, files: [files[0]]},
      token: 'synthetic',
      directory,
    }
    assert.equal(
      (await processNativeDeclarationFiles(input, dependencies)).batchCount,
      1,
    )
    assert.equal(sends, 1)
    padding = 'x'.repeat(REUNION_BATCH_MAX_BYTES)
    sends = 0
    await assert.rejects(
      processNativeDeclarationFiles(input, dependencies),
      /série native dépasse 48 MiB/,
    )
    assert.equal(sends, 0)
    assert.deepEqual(await readdir(directory), [])
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})
