import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import type {CellObject} from 'xlsx'
import XLSX from './xlsx.js'
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

void test('la normalisation des usages préserve les séparateurs et traite les espaces longs sans regex quadratique', () => {
  for (const separator of ['-', ':', '–', '—']) {
    assert.deepEqual(resolveTemplateWaterUse(`4D${separator}Refroidissement`), {
      code: '4D',
      status: 'matched',
    })
    assert.deepEqual(
      resolveTemplateWaterUse(`4D \t ${separator} \n Refroidissement`),
      {code: '4D', status: 'matched'},
    )
  }

  assert.deepEqual(
    resolveTemplateWaterUse(
      `refroidissement (> 99${' '.repeat(100_000)}% de restitution)`,
    ),
    {code: '4D', status: 'matched'},
  )
  assert.equal(resolveTemplateWaterUse('4D -').status, 'unknown')
  assert.equal(resolveTemplateWaterUse('999 - Inconnu').status, 'unknown')
  assert.equal(resolveTemplateWaterUse('4D·Refroidissement').status, 'unknown')
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

void test('sépare deux codes comptage au même PP et préserve les zéros initiaux', async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'template-codes-'))
  t.after(async () => fs.rm(directory, {recursive: true, force: true}))
  const sourceFile = path.join(directory, 'codes.xlsx')
  const workbook = XLSX.utils.book_new()
  const worksheet = XLSX.utils.json_to_sheet(
    ['001', '002', ''].map((countingCode, index) => ({
      id_point_de_prelevement: 'POINT-1',
      code_comptage: countingCode,
      date_debut: '2026-07-01',
      date_fin: '2026-07-31',
      volume_m3: 10 * (index + 1),
      usage: '2',
    })),
  )
  XLSX.utils.book_append_sheet(workbook, worksheet, 'declaration_de_volume')
  XLSX.writeFile(workbook, sourceFile)
  const context = {
    serviceAccount: 'synthetic-test',
    sourcePointId: 'POINT-1',
    rate: 100,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
    sourceFile,
  }
  const connector = new TemplateFileConnector()
  const output = await connector.run(context)
  assert.deepEqual(
    output.data.metrics.map((metric) => ({
      countingCode: metric.countingCode,
      values: metric.values.map((value) => value.value),
    })),
    [
      {countingCode: '001', values: [10]},
      {countingCode: '002', values: [20]},
      {countingCode: undefined, values: [30]},
    ],
  )
  await assert.rejects(
    connector.run({...context, connectorId: 'connector-1'}),
    /ambiguë/,
  )
  await assert.rejects(
    connector.run({
      ...context,
      connectorId: 'connector-1',
      countingCode: '001',
    }),
    /ambiguë/,
  )
  worksheet['!ref'] = 'A1:F3'
  XLSX.writeFile(workbook, sourceFile)
  await assert.rejects(
    connector.run({...context, connectorId: 'connector-1'}),
    /Plusieurs comptages/,
  )
  const scoped = await connector.run({
    ...context,
    connectorId: 'connector-1',
    exploitationId: 'exploitation-1',
    countingCode: '001',
  })
  assert.equal(scoped.exploitationId, 'exploitation-1')
  assert.equal(scoped.countingCode, '001')
  assert.ok(
    scoped.data.metrics.every((metric) => metric.countingCode === '001'),
  )
  assert.ok(
    scoped.data.metrics.every(
      (metric) => metric.exploitationId === 'exploitation-1',
    ),
  )
  assert.deepEqual(
    scoped.data.metrics.flatMap((metric) =>
      metric.values.map((value) => value.value),
    ),
    [10],
  )
})

void test('accepte le libellé Code comptage et les cellules numériques formatées en texte', async (t) => {
  const directory = await fs.mkdtemp(
    path.join(os.tmpdir(), 'template-code-text-'),
  )
  t.after(async () => fs.rm(directory, {recursive: true, force: true}))
  const sourceFile = path.join(directory, 'codes.xlsx')
  const workbook = XLSX.utils.book_new()
  const worksheet = XLSX.utils.aoa_to_sheet([
    [
      'id_point_de_prelevement',
      'Code comptage',
      'date_debut',
      'date_fin',
      'volume_m3',
    ],
    ['POINT-1', 12, '2026-07-01', '2026-07-31', 10],
  ])
  ;(worksheet.B2 as CellObject).z = '00000'
  XLSX.utils.book_append_sheet(workbook, worksheet, 'declaration_de_volume')
  XLSX.writeFile(workbook, sourceFile)
  const output = await new TemplateFileConnector().run({
    serviceAccount: 'synthetic-test',
    sourcePointId: 'POINT-1',
    rate: 100,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
    sourceFile,
  })
  assert.equal(output.data.metrics[0].countingCode, '00012')
  assert.equal(output.data.metrics[0].values[0].value, 10)
})
