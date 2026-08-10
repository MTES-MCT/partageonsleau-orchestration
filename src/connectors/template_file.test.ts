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
import {resolveTemplateWaterUse} from './template_file_water_uses.js'
import {ConflictPolicy, Granularity} from './types.js'

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000

const HISTORICAL_TEMPLATE_USAGE_CASES = [
  ['INCONNU', '0'],
  ["PAS D'USAGE", '1'],
  ['IRRIGATION', '2'],
  ['Irrigation par aspersion', '2A'],
  ['Irrigation gravitaire', '2B'],
  ['Irrigation au goutte à goutte', '2C'],
  ['Irrigation par tout autre procédé', '2D'],
  ['Lutte antigel de cultures pérennes', '2E'],
  ['AGRICULTURE-ELEVAGE (hors irrigation)', '3'],
  ['Abreuvage', '3A'],
  ['Aquaculture', '3B'],
  ['INDUSTRIE', '4'],
  ['Agro-alimentaire', '4A'],
  ['Industrie hors agro-alimentaire', '4B'],
  ['Exhaure', '4C'],
  ['Refroidissement (> 99% de restitution)', '4D'],
  ['AEP', '5'],
  ['Alimentation collective', '5A'],
  ['Alimentation individuelle', '5B'],
  ['ENERGIE', '6'],
  ['Pompe à chaleur', '6A'],
  ['Géothermie', '6B'],
  ["Refroidissement de centrales de production d'énergie", '6C'],
  ['Refroidissement de centrales thermiques', '6C1'],
  ['Refroidissement de centrales nucléaires', '6C2'],
  ['Refroidissement des centrales de production électrique', '6C3'],
  ['Barrages hydro-électriques (force motrice)', '6D'],
  ['LOISIRS', '7'],
  ['Piscine', '7A'],
  ['Baignade', '7B'],
  ['Autres activités de loisir', '7C'],
  ['Arrosage (activités de loisir)', '7D'],
  ['Canon à neige', '7E'],
] as const

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

void test('associe tous les libelles du modele historique aux codes SANDRE', () => {
  for (const [label, expectedCode] of HISTORICAL_TEMPLATE_USAGE_CASES) {
    assert.deepEqual(resolveTemplateWaterUse(label), {
      code: expectedCode,
      status: 'matched',
    })
  }
})

void test('accepte les codes prefixes et normalise les variantes typographiques', () => {
  assert.deepEqual(
    resolveTemplateWaterUse(
      '4D - Refroidissement avec restitution supérieure à 99 %',
    ),
    {code: '4D', status: 'matched'},
  )
  assert.deepEqual(
    resolveTemplateWaterUse(' refroidissement (> 99 % de restitution) '),
    {code: '4D', status: 'matched'},
  )
  assert.deepEqual(resolveTemplateWaterUse('6c2'), {
    code: '6C2',
    status: 'matched',
  })
})

void test('distingue une cellule vide d un libelle inconnu', () => {
  assert.deepEqual(resolveTemplateWaterUse(''), {
    code: undefined,
    status: 'empty',
  })
  assert.deepEqual(resolveTemplateWaterUse(undefined), {
    code: undefined,
    status: 'empty',
  })
  assert.deepEqual(resolveTemplateWaterUse('Usage fournisseur non référencé'), {
    code: '0',
    status: 'unknown',
    rawValue: 'Usage fournisseur non référencé',
  })
  assert.deepEqual(resolveTemplateWaterUse('Soutien d’étiage'), {
    code: '0',
    status: 'unknown',
    rawValue: 'Soutien d’étiage',
  })
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
      usage: 'Refroidissement (> 99% de restitution)',
    },
    {
      id_point_de_prelevement: 'POINT-1',
      date_debut: '2026-08-01',
      date_fin: '2026-08-01',
      volume_m3: 10,
      usage: 'Usage fournisseur non référencé',
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
  assert.equal(monthlyMetric.usage, '4D')
  assert.equal(dailyMetric.usage, '0')
  assert.deepEqual(output.data.source_metadata, {
    provider: 'template_file',
    sheet_name: 'declaration_de_volume',
    row_count: 2,
    unknown_usage_count: 1,
    unknown_usage_values: ['Usage fournisseur non référencé'],
  })
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
