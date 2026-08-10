import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import * as XLSX from 'xlsx'
import {
  getExclusiveTemplatePeriodEnd,
  inferTemplateGranularity,
  normalizeTemplateDateOnly,
  TemplateFileConnector,
} from './template_file.js'
import {ConflictPolicy, Granularity} from './types.js'

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000

function withTimezone(timezone: string, callback: () => void): void {
  const previousTimezone = process.env.TZ
  process.env.TZ = timezone

  try {
    callback()
  } finally {
    process.env.TZ = previousTimezone
  }
}

void test('normalise une date Excel convertie avec le decalage horaire d hiver', () => {
  const normalized = normalizeTemplateDateOnly(
    new Date('2024-11-30T22:59:39.000Z'),
  )

  assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
})

void test('normalise une date Excel convertie avec le decalage horaire d ete', () => {
  const normalized = normalizeTemplateDateOnly(
    new Date('2025-08-31T21:59:39.000Z'),
  )

  assert.equal(normalized?.toISOString(), '2025-09-01T00:00:00.000Z')
})

void test('preserve une date textuelle deja positionnee a minuit UTC', () => {
  const normalized = normalizeTemplateDateOnly('2025-06-01')

  assert.equal(normalized?.toISOString(), '2025-06-01T00:00:00.000Z')
})

void test('produit une duree en jours entiers entre deux dates du template', () => {
  const periodStart = normalizeTemplateDateOnly(
    new Date('2025-03-25T22:59:39.000Z'),
  )
  const periodEnd = normalizeTemplateDateOnly(
    new Date('2025-05-11T21:59:39.000Z'),
  )

  assert.ok(periodStart)
  assert.ok(periodEnd)
  assert.equal(
    (periodEnd.getTime() - periodStart.getTime()) / MILLISECONDS_PER_DAY,
    47,
  )
})

void test('neutralise aussi les fuseaux civils extremes appliques par xlsx', () => {
  withTimezone('Pacific/Kiritimati', () => {
    const normalized = normalizeTemplateDateOnly(
      new Date('2024-11-30T10:00:20.000Z'),
    )
    assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
  })

  withTimezone('Etc/GMT+12', () => {
    const normalized = normalizeTemplateDateOnly(
      new Date('2024-12-01T12:00:00.000Z'),
    )
    assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
  })
})

void test('convertit la date de fin inclusive en borne exclusive', () => {
  const periodEnd = getExclusiveTemplatePeriodEnd(
    new Date('2026-07-31T00:00:00.000Z'),
  )

  assert.equal(periodEnd.toISOString(), '2026-08-01T00:00:00.000Z')
})

void test('deduit la granularite de la periode civile', () => {
  assert.equal(
    inferTemplateGranularity(
      new Date('2026-07-01T00:00:00.000Z'),
      new Date('2026-07-31T00:00:00.000Z'),
    ),
    Granularity.MONTH,
  )
  assert.equal(
    inferTemplateGranularity(
      new Date('2026-07-06T00:00:00.000Z'),
      new Date('2026-07-12T00:00:00.000Z'),
    ),
    Granularity.WEEK,
  )
  assert.equal(
    inferTemplateGranularity(
      new Date('2026-07-13T00:00:00.000Z'),
      new Date('2026-07-13T00:00:00.000Z'),
    ),
    Granularity.DAY,
  )
})

void test('produit des periodes semi-ouvertes et remplace les donnees hors Willie', async (t) => {
  const temporaryDirectory = await fs.mkdtemp(
    path.join(os.tmpdir(), 'template-file-'),
  )
  const filePath = path.join(temporaryDirectory, 'template.xlsx')
  const workbook = XLSX.utils.book_new()
  const worksheet = XLSX.utils.json_to_sheet([
    {
      id_point_de_prelevement: 'POINT-1',
      date_debut: '2026-07-01',
      date_fin: '2026-07-31',
      volume_m3: 310,
    },
    {
      id_point_de_prelevement: 'POINT-1',
      date_debut: '2026-08-01',
      date_fin: '2026-08-01',
      volume_m3: 10,
    },
  ])
  XLSX.utils.book_append_sheet(workbook, worksheet, 'declaration_de_volume')
  XLSX.writeFile(workbook, filePath)
  t.after(async () => fs.rm(temporaryDirectory, {recursive: true, force: true}))

  const output = await new TemplateFileConnector().run({
    serviceAccount: 'test',
    sourcePointId: 'POINT-1',
    rate: 100,
    sourceFile: filePath,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
  })
  const monthlyMetric = output.data.metrics.find(
    (metric) => metric.granularity === Granularity.MONTH,
  )
  const dailyMetric = output.data.metrics.find(
    (metric) => metric.granularity === Granularity.DAY,
  )

  assert.ok(monthlyMetric)
  assert.ok(dailyMetric)
  assert.equal(
    monthlyMetric.conflictPolicy,
    ConflictPolicy.REPLACE_EXISTING_EXCEPT_WILLIE,
  )
  assert.equal(
    monthlyMetric.values[0]?.periodStart?.toISOString(),
    '2026-07-01T00:00:00.000Z',
  )
  assert.equal(
    monthlyMetric.values[0]?.periodEnd?.toISOString(),
    '2026-08-01T00:00:00.000Z',
  )
  assert.equal(
    dailyMetric.values[0]?.periodEnd?.toISOString(),
    '2026-08-02T00:00:00.000Z',
  )
})
