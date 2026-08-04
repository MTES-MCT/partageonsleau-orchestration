import assert from 'node:assert/strict'
import {mkdtemp, rm} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import test from 'node:test'
import xlsx from 'xlsx'
import {GidafConnector} from './gidaf.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  PointFlowType,
} from './types.js'

type MeasurementCell = Date | number | string
type PrelevementRow = [MeasurementCell, number]

type GidafFixture = {
  directory: string
  cadresPath: string
  prelevementsPath: string
}

const EXCEL_EPOCH_UTC = Date.UTC(1899, 11, 30)
const DAY_IN_MS = 24 * 60 * 60 * 1000

function excelSerial(year: number, month: number, day: number): number {
  return (Date.UTC(year, month - 1, day) - EXCEL_EPOCH_UTC) / DAY_IN_MS
}

function writeWorkbook(filePath: string, rows: unknown[][]): void {
  const worksheet = xlsx.utils.aoa_to_sheet(rows)
  const workbook = xlsx.utils.book_new()

  xlsx.utils.book_append_sheet(workbook, worksheet, 'GIDAF')
  xlsx.writeFile(workbook, filePath)
}

async function createGidafFixture(
  rows: PrelevementRow[],
): Promise<GidafFixture> {
  const directory = await mkdtemp(path.join(tmpdir(), 'gidaf-test-'))
  const cadresPath = path.join(directory, 'Cadres.xlsx')
  const prelevementsPath = path.join(directory, 'Prelevements.xlsx')

  writeWorkbook(cadresPath, [
    ['Code Inspection', 'Point de surveillance', 'Type de point'],
    ['ICPE-1', 'POINT-1', 'Alimentation'],
  ])
  writeWorkbook(prelevementsPath, [
    [
      'Code Inspection',
      'Point de surveillance',
      'Type de point',
      'Date de mesure',
      'Volume (m3)',
    ],
    ...rows.map(([measurementMonth, volume]) => [
      'ICPE-1',
      'POINT-1',
      'Alimentation',
      measurementMonth,
      volume,
    ]),
  ])

  return {directory, cadresPath, prelevementsPath}
}

function sourceFiles(fixture: GidafFixture) {
  return [
    {
      type: 'gidaf-cadres',
      filename: 'Cadres.xlsx',
      path: fixture.cadresPath,
    },
    {
      type: 'gidaf-prelevements',
      filename: 'Prelevements.xlsx',
      path: fixture.prelevementsPath,
    },
  ]
}

async function runFixture(fixture: GidafFixture) {
  const connector = new GidafConnector()
  const files = sourceFiles(fixture)
  const discoveredSourcePointIds = await connector.discoverSourcePointIds({
    sourceFiles: files,
  })
  const sourcePointId = discoveredSourcePointIds[0]

  assert.ok(sourcePointId)

  return connector.run({
    serviceAccount: 'test',
    flowType: PointFlowType.PRELEVEMENT,
    sourcePointId,
    rate: 100,
    sourceFiles: files,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
  })
}

function serializeValues(
  values: Array<{
    date: Date
    periodStart?: Date
    periodEnd?: Date
    value: number
  }>,
) {
  return values.map((value) => ({
    date: value.date.toISOString(),
    periodStart: value.periodStart?.toISOString(),
    periodEnd: value.periodEnd?.toISOString(),
    value: value.value,
  }))
}

void test('GIDAF convertit chaque date de mesure en mois civil semi-ouvert', async (t) => {
  const fixture = await createGidafFixture([
    [excelSerial(2025, 6, 1), 10],
    [excelSerial(2025, 6, 15), 20],
    [excelSerial(2025, 6, 30), 30],
    [excelSerial(2025, 8, 17), 40],
  ])
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const output = await runFixture(fixture)
  const [metric] = output.data.metrics

  assert.equal(output.data.min_date?.toISOString(), '2025-06-01T00:00:00.000Z')
  assert.equal(output.data.max_date?.toISOString(), '2025-09-01T00:00:00.000Z')
  assert.equal(output.data.source_metadata?.row_count, 4)
  assert.equal(metric?.type, MetricType.VOLUME)
  assert.equal(metric?.unit, MetricUnit.M3)
  assert.equal(metric?.granularity, Granularity.MONTH)
  assert.equal(metric?.conflictPolicy, ConflictPolicy.REPLACE_EXISTING)
  assert.deepEqual(serializeValues(metric?.values ?? []), [
    {
      date: '2025-07-01T00:00:00.000Z',
      periodStart: '2025-06-01T00:00:00.000Z',
      periodEnd: '2025-07-01T00:00:00.000Z',
      value: 60,
    },
    {
      date: '2025-09-01T00:00:00.000Z',
      periodStart: '2025-08-01T00:00:00.000Z',
      periodEnd: '2025-09-01T00:00:00.000Z',
      value: 40,
    },
  ])
})

void test('GIDAF accepte les mois nommés en français et en anglais', async (t) => {
  const fixture = await createGidafFixture([
    ['juin-25', 10],
    ['July-2025', 20],
    ['févr.-2026', 30],
    ['March 2026', 40],
    ['04/2026', 50],
    ['05-26', 60],
  ])
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const output = await runFixture(fixture)
  const values = output.data.metrics[0]?.values ?? []

  assert.deepEqual(serializeValues(values), [
    {
      date: '2025-07-01T00:00:00.000Z',
      periodStart: '2025-06-01T00:00:00.000Z',
      periodEnd: '2025-07-01T00:00:00.000Z',
      value: 10,
    },
    {
      date: '2025-08-01T00:00:00.000Z',
      periodStart: '2025-07-01T00:00:00.000Z',
      periodEnd: '2025-08-01T00:00:00.000Z',
      value: 20,
    },
    {
      date: '2026-03-01T00:00:00.000Z',
      periodStart: '2026-02-01T00:00:00.000Z',
      periodEnd: '2026-03-01T00:00:00.000Z',
      value: 30,
    },
    {
      date: '2026-04-01T00:00:00.000Z',
      periodStart: '2026-03-01T00:00:00.000Z',
      periodEnd: '2026-04-01T00:00:00.000Z',
      value: 40,
    },
    {
      date: '2026-05-01T00:00:00.000Z',
      periodStart: '2026-04-01T00:00:00.000Z',
      periodEnd: '2026-05-01T00:00:00.000Z',
      value: 50,
    },
    {
      date: '2026-06-01T00:00:00.000Z',
      periodStart: '2026-05-01T00:00:00.000Z',
      periodEnd: '2026-06-01T00:00:00.000Z',
      value: 60,
    },
  ])
})

void test('GIDAF conserve les formats de dates textuelles déjà acceptés', async (t) => {
  const fixture = await createGidafFixture([
    ['01/04/2025', 10],
    ['2025-05-31', 20],
    ['15-06-2025', 30],
  ])
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const output = await runFixture(fixture)

  assert.deepEqual(serializeValues(output.data.metrics[0]?.values ?? []), [
    {
      date: '2025-05-01T00:00:00.000Z',
      periodStart: '2025-04-01T00:00:00.000Z',
      periodEnd: '2025-05-01T00:00:00.000Z',
      value: 10,
    },
    {
      date: '2025-06-01T00:00:00.000Z',
      periodStart: '2025-05-01T00:00:00.000Z',
      periodEnd: '2025-06-01T00:00:00.000Z',
      value: 20,
    },
    {
      date: '2025-07-01T00:00:00.000Z',
      periodStart: '2025-06-01T00:00:00.000Z',
      periodEnd: '2025-07-01T00:00:00.000Z',
      value: 30,
    },
  ])
})

void test('GIDAF ignore une année seule au lieu de la lire comme une date', async (t) => {
  const fixture = await createGidafFixture([
    [2025, 15_500],
    ['2025', 500],
    [excelSerial(2025, 4, 15), 250],
  ])
  t.after(async () => {
    await rm(fixture.directory, {recursive: true, force: true})
  })

  const output = await runFixture(fixture)

  assert.equal(output.data.source_metadata?.row_count, 1)
  assert.equal(output.data.min_date?.toISOString(), '2025-04-01T00:00:00.000Z')
  assert.equal(output.data.max_date?.toISOString(), '2025-05-01T00:00:00.000Z')
  assert.deepEqual(serializeValues(output.data.metrics[0]?.values ?? []), [
    {
      date: '2025-05-01T00:00:00.000Z',
      periodStart: '2025-04-01T00:00:00.000Z',
      periodEnd: '2025-05-01T00:00:00.000Z',
      value: 250,
    },
  ])
})
