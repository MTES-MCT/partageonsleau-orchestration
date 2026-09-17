import assert from 'node:assert/strict'
import {Buffer} from 'node:buffer'
import {execFile} from 'node:child_process'
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import process from 'node:process'
import test, {type TestContext} from 'node:test'
import {fileURLToPath} from 'node:url'
import {promisify} from 'node:util'
import type {CellObject} from 'xlsx'
import {AquasysConnector} from './aquasys.js'
import {BvTechConnector} from './bv-tech.js'
import {readSpreadsheetSheet} from './spreadsheet.js'
import {MetricType, PointFlowType} from './types.js'
import xlsx from './xlsx.js'

const execFileAsync = promisify(execFile)

async function fixtureFile(
  t: TestContext,
  format: 'xls' | 'xlsx',
  kind = 'spreadsheets',
) {
  const fixtures = JSON.parse(
    await readFile(
      new URL(`fixtures/${kind}-legacy.json`, import.meta.url),
      'utf8',
    ),
  ) as Record<string, string>
  const directory = await mkdtemp(path.join(tmpdir(), 'spreadsheet-compat-'))
  t.after(async () => rm(directory, {recursive: true, force: true}))
  const sourceFile = path.join(directory, `historique.${format}`)
  await writeFile(sourceFile, Buffer.from(fixtures[format], 'base64'))
  return sourceFile
}

function context(sourceFile: string, sourcePointId: string) {
  return {
    sourceFile,
    sourcePointId,
    serviceAccount: 'synthetic-test',
    flowType: PointFlowType.PRELEVEMENT,
    rate: 100,
    mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
  }
}

async function formattedBvTechFixture(
  t: TestContext,
  format: 'xls' | 'xlsx',
  date1904: boolean,
) {
  const directory = await mkdtemp(path.join(tmpdir(), 'bv-tech-dates-'))
  t.after(async () => rm(directory, {recursive: true, force: true}))
  const sourceFile = path.join(directory, `dates.${format}`)
  // Fixed Excel serials, not Date objects written by the parser under test:
  // 02/01/2026 00:00, 02/06/2026 00:00 and 03/06/2026 12:30.
  const epochOffset = date1904 ? 1462 : 0
  const sheet = xlsx.utils.aoa_to_sheet([
    ['', 'FORAGE-É', 'FORAGE-É'],
    ['', 'POINT-BV', 'POINT-BV'],
    ['Contexte'],
    ['Horodatage', 'Volume journalier', 'Débit'],
    ['Descriptions'],
    ['', 'm³', 'm³/h'],
    [46024 - epochOffset, 12.5, 36],
    [46175 - epochOffset, 15, 72],
    [46176 + 12.5 / 24 - epochOffset, 17.5, 108],
  ])
  for (const address of ['A7', 'A8', 'A9']) {
    const cell = sheet[address] as CellObject
    cell.z = 'dd/mm/yyyy hh:mm:ss'
  }

  const workbook = xlsx.utils.book_new()
  workbook.Workbook = {WBProps: {date1904}}
  xlsx.utils.book_append_sheet(workbook, sheet, 'Mesures été')
  xlsx.writeFile(workbook, sourceFile, {bookType: format})
  return sourceFile
}

async function assertBvTechDatesInTimezone(
  sourceFile: string,
  timezone: 'UTC' | 'Europe/Paris',
  dateSystem: '1900' | '1904' | 'legacy',
) {
  // Start a fresh process: SheetJS date handling must not depend on another
  // test changing TZ after the module has already been loaded.
  await execFileAsync(
    process.execPath,
    [
      '--import',
      'tsx',
      fileURLToPath(
        new URL('fixtures/bv-tech-date-assertions.ts', import.meta.url),
      ),
      sourceFile,
      dateSystem,
    ],
    {env: {...process.env, TZ: timezone}, timeout: 10_000},
  )
}

void test('le parseur officiel corrigé expose le support fichiers et encodages historiques', () => {
  assert.equal(xlsx.version, '0.20.3')
  assert.equal(typeof xlsx.readFile, 'function')
  assert.equal(typeof xlsx.writeFile, 'function')
})

for (const format of ['xls', 'xlsx'] as const) {
  for (const timezone of ['UTC', 'Europe/Paris'] as const) {
    for (const date1904 of [false, true]) {
      const dateSystem = date1904 ? '1904' : '1900'
      void test(`BV-Tech conserve les jours et heures Excel en ${format}, ${timezone}, système ${dateSystem}`, async (t) => {
        const sourceFile = await formattedBvTechFixture(t, format, date1904)
        await assertBvTechDatesInTimezone(sourceFile, timezone, dateSystem)
      })
    }

    void test(`BV-Tech conserve les dates numériques et textuelles en ${format}, ${timezone}`, async (t) => {
      const sourceFile = await fixtureFile(t, format, 'bv-tech')
      await assertBvTechDatesInTimezone(sourceFile, timezone, 'legacy')
    })
  }

  void test(`Aquasys conserve les index, coefficients et volumes du fichier historique ${format}`, async (t) => {
    const sourceFile = await fixtureFile(t, format)
    const connector = new AquasysConnector()
    assert.deepEqual(await connector.discoverSourcePointIds({sourceFile}), [
      'INDEX-É',
      'VOLUME-É||CLIENT-%C3%89',
    ])

    const indexOutput = await connector.run(context(sourceFile, 'INDEX-É'))
    const index = indexOutput.data.metrics[0]
    assert.equal(index.type, MetricType.VOLUME)
    assert.deepEqual(
      index.values.map((value) => ({
        start: value.periodStart?.toISOString(),
        end: value.periodEnd?.toISOString(),
        value: value.value,
      })),
      [
        {
          start: '2026-01-01T00:00:00.000Z',
          end: '2026-02-01T00:00:00.000Z',
          value: 100,
        },
      ],
    )

    const volumeOutput = await connector.run(
      context(sourceFile, 'VOLUME-É||CLIENT-%C3%89'),
    )
    assert.equal(volumeOutput.data.metrics[0].values[0].value, 123.5)
    assert.deepEqual(volumeOutput.data.source_metadata?.externalDeclarant, {
      sourceId: 'CLIENT-É',
      name: 'Préleveur été',
      siret: undefined,
    })
  })

  void test(`BV-Tech conserve les dates Excel, accents, volumes et unités du fichier historique ${format}`, async (t) => {
    const sourceFile = await fixtureFile(t, format, 'bv-tech')
    const connector = new BvTechConnector()
    assert.deepEqual(await connector.discoverSourcePointIds({sourceFile}), [
      'FORAGE-É',
    ])
    const output = await connector.run(context(sourceFile, 'FORAGE-É'))
    const volume = output.data.metrics.find(
      (metric) => metric.type === MetricType.VOLUME,
    )
    assert.deepEqual(
      volume?.values.map((value) => ({
        date: value.date.toISOString(),
        value: value.value,
      })),
      [
        {date: '2026-01-01T00:00:00.000Z', value: 12.5},
        {date: '2026-01-02T00:00:00.000Z', value: 15},
      ],
    )
    const debit = output.data.metrics.find(
      (metric) => metric.type === MetricType.DEBIT,
    )
    assert.deepEqual(
      debit?.values.map((value) => value.value),
      [10, 20],
    )
  })

  void test(`le lecteur partagé conserve les cellules vides et refuse une feuille requise absente en ${format}`, async (t) => {
    const sourceFile = await fixtureFile(t, format)
    const options = {connectorName: 'synthetic-test', required: true}
    const sheet = await readSpreadsheetSheet(sourceFile, 'Vide', options)
    assert.deepEqual(sheet, {
      headers: ['Nom', 'Date', 'Valeur'],
      rows: [{Nom: 'Été', Date: '', Valeur: ''}],
    })
    await assert.rejects(
      readSpreadsheetSheet(sourceFile, 'Absente', options),
      /not found/v,
    )
  })
}

void test('Aquasys lit un CSV français avec BOM, point-virgule et virgule décimale', async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), 'aquasys-csv-'))
  t.after(async () => rm(directory, {recursive: true, force: true}))
  const sourceFile = path.join(directory, 'export.csv')
  await writeFile(
    sourceFile,
    '\u{FEFF}Point de prélèvement;Index ou volume;Date de mesure;Date de fin;Mesure\r\nFORAGE-É;volume;01/06/2026;01/07/2026;123,5\r\n',
  )
  const connector = new AquasysConnector()
  assert.deepEqual(await connector.discoverSourcePointIds({sourceFile}), [
    'FORAGE-É',
  ])
  const output = await connector.run(context(sourceFile, 'FORAGE-É'))
  assert.equal(output.data.metrics[0].values[0].value, 123.5)
  assert.equal(
    output.data.metrics[0].values[0].periodStart?.toISOString(),
    '2026-06-01T00:00:00.000Z',
  )
  assert.equal(
    output.data.metrics[0].values[0].periodEnd?.toISOString(),
    '2026-07-01T00:00:00.000Z',
  )
})
