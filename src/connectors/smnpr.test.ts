import assert from 'node:assert/strict'
import {mkdtemp, rm} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import test from 'node:test'
import * as XLSX from 'xlsx'
import {SmnprConnector} from './smnpr.js'
import {TemplateFileConnector} from './template_file.js'
import {ConflictPolicy, MetricType, MetricUnit, PointFlowType} from './types.js'
import {connectorRegistry} from './index.js'

const EXPECTED_HEADERS = [
  'id_point_de_prelevement_ou_rejet',
  'date_debut',
  'date_fin',
  'volume_preleve_m3',
]

type FixtureRow = [string, string, string, number | string]

async function createWorkbookFixture(parameters: {
  headers?: string[]
  rows: FixtureRow[]
  sheetName?: string
}): Promise<{directory: string; filePath: string}> {
  const directory = await mkdtemp(path.join(tmpdir(), 'smnpr-test-'))
  const filePath = path.join(directory, 'declaration.xlsx')
  const worksheet = XLSX.utils.aoa_to_sheet([
    parameters.headers ?? EXPECTED_HEADERS,
    ...parameters.rows,
  ])
  const workbook = XLSX.utils.book_new()

  XLSX.utils.book_append_sheet(
    workbook,
    worksheet,
    parameters.sheetName ?? 'declaration_de_volume',
  )
  XLSX.writeFile(workbook, filePath)

  return {directory, filePath}
}

function connectorContext(filePath: string, sourcePointId = 'BSS002MUNP') {
  return {
    serviceAccount: 'test',
    flowType: PointFlowType.PRELEVEMENT,
    sourcePointId,
    rate: 100,
    sourceFile: filePath,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
  }
}

void test('le connecteur SMNPR est enregistré', () => {
  assert.equal(connectorRegistry.get('smnpr')?.name, 'smnpr')
})

void test('les utilitaires partagés préservent le parser template', async (t) => {
  const fixture = await createWorkbookFixture({
    rows: [['BSS002MUNP', '2026-06-01', '2026-07-01', 1250]],
  })
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const output = await new TemplateFileConnector().run(
    connectorContext(fixture.filePath),
  )
  const [metric] = output.data.metrics

  assert.equal(output.connector, 'template_file')
  assert.equal(metric?.values.length, 1)
  assert.equal(metric?.conflictPolicy, ConflictPolicy.SKIP_CONFLICTING_VALUES)
})

void test('le connecteur découvre les BSS et produit les volumes par période', async (t) => {
  const fixture = await createWorkbookFixture({
    rows: [
      ['BSS002MUNP', '2026-06-01', '2026-07-01', 1250.5],
      ['BSS002MUNP', '2026-07-01', '2026-08-01', 0],
      ['BSS002MUWB', '2026-06-01', '2026-07-01', 800],
    ],
  })
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const connector = new SmnprConnector()
  const sourcePointIds = await connector.discoverSourcePointIds({
    sourceFile: fixture.filePath,
  })
  const output = await connector.run(connectorContext(fixture.filePath))
  const [metric] = output.data.metrics

  assert.deepEqual(sourcePointIds, ['BSS002MUNP', 'BSS002MUWB'])
  assert.equal(output.connector, 'smnpr')
  assert.equal(output.data.flow_type, PointFlowType.PRELEVEMENT)
  assert.equal(output.data.source_metadata?.provider, 'smnpr')
  assert.equal(output.data.source_metadata?.row_count, 2)
  assert.equal(output.data.min_date?.toISOString(), '2026-06-01T00:00:00.000Z')
  assert.equal(output.data.max_date?.toISOString(), '2026-08-01T00:00:00.000Z')
  assert.equal(metric?.type, MetricType.VOLUME)
  assert.equal(metric?.unit, MetricUnit.M3)
  assert.equal(metric?.conflictPolicy, ConflictPolicy.REPLACE_EXISTING)
  assert.deepEqual(
    metric?.values.map((value) => ({
      date: value.date.toISOString(),
      periodStart: value.periodStart?.toISOString(),
      periodEnd: value.periodEnd?.toISOString(),
      value: value.value,
    })),
    [
      {
        date: '2026-07-01T00:00:00.000Z',
        periodStart: '2026-06-01T00:00:00.000Z',
        periodEnd: '2026-07-01T00:00:00.000Z',
        value: 1250.5,
      },
      {
        date: '2026-08-01T00:00:00.000Z',
        periodStart: '2026-07-01T00:00:00.000Z',
        periodEnd: '2026-08-01T00:00:00.000Z',
        value: 0,
      },
    ],
  )
})

void test('le connecteur exige les quatre colonnes SMNPR dans le bon ordre', async (t) => {
  const fixture = await createWorkbookFixture({
    headers: [...EXPECTED_HEADERS].toReversed(),
    rows: [['BSS002MUNP', '2026-06-01', '2026-07-01', 1250]],
  })
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  await assert.rejects(
    new SmnprConnector().discoverSourcePointIds({
      sourceFile: fixture.filePath,
    }),
    /En-têtes invalides/v,
  )
})

const invalidRows: Array<{
  label: string
  row: FixtureRow
  error: RegExp
}> = [
  {
    label: 'un code BSS invalide',
    row: ['POINT-1', '2026-06-01', '2026-07-01', 1250],
    error: /code BSS invalide/v,
  },
  {
    label: 'une date de début invalide',
    row: ['BSS002MUNP', 'inconnue', '2026-07-01', 1250],
    error: /date de début invalide/v,
  },
  {
    label: 'une période inversée',
    row: ['BSS002MUNP', '2026-07-01', '2026-06-01', 1250],
    error: /date de fin doit être postérieure/v,
  },
  {
    label: 'un volume négatif',
    row: ['BSS002MUNP', '2026-06-01', '2026-07-01', -1],
    error: /volume prélevé doit être un nombre positif ou nul/v,
  },
  {
    label: 'un volume non numérique',
    row: ['BSS002MUNP', '2026-06-01', '2026-07-01', 'inconnu'],
    error: /volume prélevé doit être un nombre positif ou nul/v,
  },
]

for (const invalidRow of invalidRows) {
  void test(`le connecteur bloque le fichier avec ${invalidRow.label}`, async (t) => {
    const fixture = await createWorkbookFixture({rows: [invalidRow.row]})
    t.after(async () => {
      await rm(fixture.directory, {recursive: true, force: true})
    })

    await assert.rejects(
      new SmnprConnector().discoverSourcePointIds({
        sourceFile: fixture.filePath,
      }),
      invalidRow.error,
    )
  })
}

void test('le connecteur bloque une période dupliquée pour un BSS', async (t) => {
  const row: FixtureRow = ['BSS002MUNP', '2026-06-01', '2026-07-01', 1250]
  const fixture = await createWorkbookFixture({rows: [row, row]})
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  await assert.rejects(
    new SmnprConnector().discoverSourcePointIds({
      sourceFile: fixture.filePath,
    }),
    /cette période est déjà déclarée/v,
  )
})

void test('le connecteur refuse d’importer un volume sur un point de rejet', async (t) => {
  const fixture = await createWorkbookFixture({
    rows: [['BSS002MUNP', '2026-06-01', '2026-07-01', 1250]],
  })
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  await assert.rejects(
    new SmnprConnector().run({
      ...connectorContext(fixture.filePath),
      flowType: PointFlowType.REJET,
    }),
    /configuré en rejet/v,
  )
})
