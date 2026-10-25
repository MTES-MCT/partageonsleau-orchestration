import assert from 'node:assert/strict'
import test from 'node:test'
import {mkdtemp, writeFile, rm} from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import type {BookType, WorkBook} from 'xlsx'
import XLSX from './xlsx.js'
import {parseReunionFile, reunionConnectorName} from './reunion.js'
import {reunionParameters} from './reunion-parameters.js'
import {
  reunionDate,
  reunionDecimal,
  reunionFrequency,
  reunionTime,
} from './reunion-values.js'
import {connectorRegistry} from './index.js'
import {
  runConnectorForDeclaration,
  resolveConnectorName,
} from '../jobs/process-declaration.js'

type Column = {
  parameter: string
  unit: string
  frequency?: string
  nature?: string
  values: unknown[]
}
function multi(
  columns: Column[],
  dates: string[] = ['01/02/2024'],
  times: unknown[] = ['00:00:00'],
): WorkBook {
  const workbook = XLSX.utils.book_new()
  const metadata = XLSX.utils.aoa_to_sheet([
    [],
    [],
    ['Point', '412 | Point synthétique'],
  ])
  XLSX.utils.book_append_sheet(workbook, metadata, 'A LIRE')
  const rows: unknown[][] = Array.from({length: 12}, () => [])
  rows[11] = [
    'Date',
    'Heure',
    ...columns.map((_, index) => `valeur_parametre_${index + 1}`),
    'Remarque',
  ]
  for (const [index, column] of columns.entries()) {
    rows[1][index + 2] = column.parameter
    rows[2][index + 2] = column.nature ?? 'valeur brute'
    rows[3][index + 2] = column.frequency ?? '1 mois'
    rows[4][index + 2] = column.unit
  }
  for (const [index, date] of dates.entries())
    rows.push([
      date,
      times[index],
      ...columns.map((column) => column.values[index]),
      'Remarque synthétique',
    ])
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet(rows),
    'Data | T=autre',
  )
  return workbook
}
function parse(
  workbook: WorkBook,
  typeCode = 'aep-zre',
  bookType: BookType = 'xlsx',
) {
  return parseReunionFile({
    buffer: XLSX.write(workbook, {type: 'buffer', bookType}) as Buffer,
    filename: `fixture.${bookType}`,
    fileId: 'fixture-id',
    typeCode,
  })
}
function camion(values: unknown[][]): WorkBook {
  const workbook = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(
    workbook,
    XLSX.utils.aoa_to_sheet([
      [],
      [],
      [
        'Date',
        '412 Riv. St Denis La Colline',
        '413 Rav. à Jacques (La Montagne)',
      ],
      ...values,
    ]),
    'Volumes',
  )
  return workbook
}

void test('reconnaît les formats Réunion et les inscrit au registre', () => {
  for (const code of [
    'camion-citerne',
    'reunion-camion-citerne',
    'reunion_camion_citerne',
    'aep-zre',
    'icpe-hors-zre',
    'reunion-multi-param',
    'reunion_multi_param',
  ]) {
    const connector = resolveConnectorName(code)
    assert.equal(connector, reunionConnectorName(code))
    assert.ok(connectorRegistry.has(connector!))
  }
})
void test('le volume mensuel reste une période native, même en février bissextile', () => {
  const result = parse(
    multi([
      {
        parameter: 'volume prélevé',
        unit: 'm3',
        values: ['1234,123456789012345'],
      },
    ]),
  )
  assert.equal(result.errors.length, 0)
  assert.equal(result.series.length, 1)
  const [series] = result.series
  assert.equal(series.pointPrelevement, 'reunion:DEP-974:point:412')
  assert.equal(series.autoCalculateVolumes, false)
  assert.equal(series.unit, 'm³')
  assert.equal(series.valueType, 'cumulative')
  assert.equal(series.data.length, 1)
  assert.deepEqual(series.data[0], {
    date: '2024-01-31T20:00:00.000Z',
    periodStart: '2024-01-31T20:00:00.000Z',
    periodEnd: '2024-02-29T20:00:00.000Z',
    value: '1234.123456789012345',
    remark: 'Remarque synthétique',
    metadata: {
      row: 13,
      cell: 'C13',
      rawValue: '1234,123456789012345',
      rawDate: '01/02/2024',
      rawTime: '00:00:00',
    },
  })
})
void test('les relevés conservent les secondes et millisecondes en UTC+4, sans durée artificielle', () => {
  const result = parse(
    multi(
      [
        {
          parameter: 'relevé d’index de compteur',
          unit: 'm³',
          frequency: '1 seconde',
          values: ['100.123456789012345', '101'],
        },
      ],
      ['25/10/2026', '25/10/2026'],
      ['02:03:04.567', '02:03:05.567'],
    ),
  )
  assert.equal(result.errors.length, 0)
  const [first, second] = result.series[0].data
  assert.equal(first.date, '2026-10-24T22:03:04.567Z')
  assert.equal(first.periodEnd, first.periodStart)
  assert.equal(second.date, '2026-10-24T22:03:05.567Z')
  assert.equal(first.value, '100.123456789012345')
})
void test('les 15 paramètres et leurs unités natives restent distincts', () => {
  for (const [parameter, config] of Object.entries(reunionParameters)) {
    for (const [unit, bounds] of Object.entries(config.units)) {
      const value = Math.max(bounds.min ?? 0, 4)
      const result = parse(multi([{parameter, unit, values: [String(value)]}]))
      assert.equal(result.errors.length, 0, parameter)
      assert.equal(result.series[0].parameter, parameter)
      assert.equal(result.series[0].unit, unit)
      assert.equal(result.series[0].data[0].value, String(value))
    }
  }
  assert.equal(Object.keys(reunionParameters).length, 15)
})
void test('le camion garde les cellules valides et signale précisément les erreurs', () => {
  const result = parse(
    camion([
      ['01/07/2026', '10,25', 'invalide'],
      ['02/07/2026', -1, 0],
      ['date invalide', 3, 4],
    ]),
    'camion-citerne',
  )
  assert.equal(result.series.length, 2)
  assert.deepEqual(
    result.series.map((series) => series.data.map((value) => value.value)),
    [['10.25'], ['0']],
  )
  assert.equal(
    result.errors.filter((error) => error.code === 'INVALID_VALUE').length,
    2,
  )
  assert.equal(
    result.errors.filter((error) => error.code === 'INVALID_DATE').length,
    2,
  )
  assert.equal(result.errors[0].filename, 'fixture.xlsx')
  assert.equal(result.errors[0].fileId, 'fixture-id')
  assert.equal(result.series[0].data[0].periodStart, '2026-06-30T20:00:00.000Z')
  assert.equal(result.series[0].data[0].periodEnd, '2026-07-01T20:00:00.000Z')
  assert.equal(result.series[0].frequency, '1 day')
})
void test('le camion conserve un cumul unique nul sur la période inclusive explicite', () => {
  for (const rawDate of [
    '01/12/2025 au 19/12/2025',
    ' 1/12/2025\u00a0AU\u202f19/12/2025 ',
    '01/12/2025au19/12/2025',
  ]) {
    const result = parse(camion([[rawDate, 0]]), 'camion-citerne')
    assert.deepEqual(result.errors, [])
    assert.equal(result.series.length, 1)
    const [series] = result.series
    assert.equal(series.frequency, 'irregular')
    assert.equal(series.valueType, 'cumulative')
    assert.equal(series.data.length, 1)
    assert.equal(series.minDate, '2025-11-30T20:00:00.000Z')
    assert.equal(series.maxDate, '2025-12-19T20:00:00.000Z')
    assert.deepEqual(series.data[0], {
      date: series.minDate,
      periodStart: series.minDate,
      periodEnd: series.maxDate,
      value: '0',
      metadata: {row: 4, cell: 'B4', rawValue: '0', rawDate: rawDate.trim()},
    })
  }
})
void test('les périodes camion invalides, inversées ou absentes ne remplacent pas les dates quotidiennes', () => {
  const invalidDates = [
    '31/02/2025 au 02/03/2025',
    '01/12/2025 au 32/12/2025',
    '19/12/2025 au 01/12/2025',
    '01/12/2025 au',
    undefined,
  ]
  const result = parse(
    camion([
      ...invalidDates.map((date) => [date, 8]),
      ['20/12/2025', '1,234567890123456789'],
    ]),
    'camion-citerne',
  )
  assert.deepEqual(
    result.errors.map((error) => error.code),
    invalidDates.map(() => 'INVALID_DATE'),
  )
  const [series] = result.series
  assert.equal(series.frequency, '1 day')
  assert.equal(series.data.length, 1)
  assert.equal(series.data[0].value, '1.234567890123456789')
  assert.equal(series.data[0].periodStart, '2025-12-19T20:00:00.000Z')
  assert.equal(series.data[0].periodEnd, '2025-12-20T20:00:00.000Z')
})
void test('les périodes camion incluent le dernier jour et ignorent les changements d’heure métropolitains', () => {
  for (const [rawDate, start, end, days] of [
    ['28/02/2024 au 01/03/2024', '2024-02-27', '2024-03-01', 3],
    ['28/03/2026 au 30/03/2026', '2026-03-27', '2026-03-30', 3],
    ['24/10/2026 au 26/10/2026', '2026-10-23', '2026-10-26', 3],
    ['29/02/2024 au 29/02/2024', '2024-02-28', '2024-02-29', 1],
  ] as const) {
    const result = parse(camion([[rawDate, 10]]), 'camion-citerne')
    assert.deepEqual(result.errors, [])
    assert.equal(result.series[0].frequency, 'irregular')
    assert.equal(result.series[0].data.length, 1)
    const [value] = result.series[0].data
    assert.equal(value.periodStart, `${start}T20:00:00.000Z`)
    assert.equal(value.periodEnd, `${end}T20:00:00.000Z`)
    assert.equal(
      Date.parse(value.periodEnd) - Date.parse(value.periodStart),
      days * 86_400_000,
    )
  }
})
void test('une série camion mixte garde chaque période native et sa borne maximale', () => {
  const result = parse(
    camion([
      ['01/12/2025 au 19/12/2025', 10],
      ['10/12/2025', 2],
    ]),
    'camion-citerne',
  )
  assert.deepEqual(result.errors, [])
  const [series] = result.series
  assert.equal(series.frequency, 'irregular')
  assert.equal(series.maxDate, '2025-12-19T20:00:00.000Z')
  assert.deepEqual(
    series.data.map(({periodStart, periodEnd, value}) => ({
      periodStart,
      periodEnd,
      value,
    })),
    [
      {
        periodStart: '2025-11-30T20:00:00.000Z',
        periodEnd: '2025-12-19T20:00:00.000Z',
        value: '10',
      },
      {
        periodStart: '2025-12-09T20:00:00.000Z',
        periodEnd: '2025-12-10T20:00:00.000Z',
        value: '2',
      },
    ],
  )
})
void test('les valeurs hors bornes et les heures manquantes ne bloquent pas les autres valeurs', () => {
  const result = parse(
    multi(
      [
        {
          parameter: 'pH',
          unit: 'autre',
          frequency: 'minute',
          values: [7, 3, 8, 9],
        },
      ],
      ['01/01/2026', '01/01/2026', '01/01/2026', '31/02/2026'],
      ['12:00:00', '12:01:00', undefined, '12:03:00'],
    ),
  )
  assert.deepEqual(
    result.series[0].data.map((value) => value.value),
    ['7'],
  )
  assert.deepEqual(
    result.errors.map((error) => error.code),
    ['INVALID_VALUE', 'INVALID_TIME', 'INVALID_DATE'],
  )
})
void test('les doublons identiques sont tracés et les contradictions sont toutes écartées', () => {
  const result = parse(
    camion([
      ['01/01/2026', 1],
      ['01/01/2026', '1.00'],
      ['02/01/2026', 2],
      ['02/01/2026', 3],
      ['03/01/2026', 4],
    ]),
    'camion-citerne',
  )
  assert.deepEqual(
    result.series[0].data.map((value) => value.value),
    ['1', '4'],
  )
  assert.equal(
    result.errors.filter((error) => error.code === 'DUPLICATE_IDENTICAL')
      .length,
    1,
  )
  assert.equal(
    result.errors.filter((error) => error.code === 'DUPLICATE_CONFLICT').length,
    2,
  )
  assert.deepEqual(
    result.errors
      .filter((error) => error.code === 'DUPLICATE_CONFLICT')
      .map((error) => error.rawValue),
    ['2', '3'],
  )
})
void test('les colonnes de points répétées ne sont pas additionnées arbitrairement', () => {
  const workbook = camion([['01/01/2026', 1, 2]])
  workbook.Sheets.Volumes.C3 = {t: 's', v: '412 Riv. St Denis La Colline'}
  const result = parse(workbook, 'camion-citerne')
  assert.equal(result.series.length, 0)
  assert.equal(
    result.errors.filter((error) => error.code === 'DUPLICATE_POINT_COLUMN')
      .length,
    2,
  )
})
void test('un PP manquant garde les valeurs pour association explicite', () => {
  const workbook = multi([
    {parameter: 'volume restitué', unit: 'm3', values: [20]},
  ])
  delete workbook.Sheets['A LIRE'].B3
  const result = parse(workbook)
  assert.equal(result.series.length, 1)
  assert.equal(result.series[0].pointPrelevement, '')
  assert.equal(result.errors[0].code, 'UNRESOLVED_POINT')
})
void test('les formats XLS et ODS restent acceptés', () => {
  for (const bookType of ['xls', 'ods'] as const) {
    const result = parse(
      multi(
        [
          {
            parameter: 'débit réservé',
            unit: 'm³/h',
            nature: 'moyenne',
            frequency: 'minute',
            values: [3.6],
          },
        ],
        ['01/01/2026'],
        ['12:00:30'],
      ),
      'aep-zre',
      bookType,
    )
    assert.equal(result.errors.length, 0, bookType)
    assert.equal(result.series[0].data[0].value, '3.6')
    assert.equal(result.series[0].data[0].periodEnd, '2026-01-01T08:01:30.000Z')
  }
})
void test('la clé de série est stable au rejeu et distincte entre pièces jointes', () => {
  const workbook = multi([{parameter: 'nitrates', unit: 'mg/L', values: [2]}])
  const buffer = XLSX.write(workbook, {
    type: 'buffer',
    bookType: 'xlsx',
  }) as Buffer
  const first = parseReunionFile({
    buffer,
    filename: 'one.xlsx',
    fileId: 'one',
    typeCode: 'aep-zre',
  })
  const replay = parseReunionFile({
    buffer,
    filename: 'renamed.xlsx',
    fileId: 'one',
    typeCode: 'aep-zre',
  })
  const other = parseReunionFile({
    buffer,
    filename: 'one.xlsx',
    fileId: 'two',
    typeCode: 'aep-zre',
  })
  assert.equal(first.series[0].seriesKey, replay.series[0].seriesKey)
  assert.notEqual(first.series[0].seriesKey, other.series[0].seriesKey)
})
void test('le transport de déclaration conserve les décimales natives et les pièces non tabulaires', async () => {
  const directory = await mkdtemp(
    path.join(os.tmpdir(), 'reunion-parser-test-'),
  )
  try {
    const file = path.join(directory, 'synthetic.xlsx')
    await writeFile(
      file,
      XLSX.write(
        multi([
          {parameter: 'autre', unit: 'autre', values: ['0.123456789012345']},
        ]),
        {type: 'buffer', bookType: 'xlsx'},
      ) as Buffer,
    )
    const attachment = path.join(directory, 'synthetic.pdf')
    await writeFile(attachment, 'synthetic')
    const result = await runConnectorForDeclaration({
      declaration: {
        id: 'synthetic',
        type: 'aep-zre',
        declarantUserId: 'synthetic-user',
        files: [],
        points: [],
      },
      localFiles: [
        {
          id: 'data-file',
          type: 'aep-zre',
          filename: 'synthetic.xlsx',
          url: 'unused',
          localPath: file,
        },
        {
          id: 'attachment',
          type: 'aep-zre',
          filename: 'synthetic.pdf',
          url: 'unused',
          localPath: attachment,
        },
      ],
    })
    assert.equal(result.data?.nativeTemporal, true)
    assert.equal(result.data?.series[0].data[0].value, '0.123456789012345')
    assert.equal(result.errors.length, 1)
    assert.equal((result.errors[0] as {code: string}).code, 'ATTACHMENT_ONLY')
  } finally {
    await rm(directory, {recursive: true, force: true})
  }
})
void test('une déclaration avec seulement des erreurs conserve son enveloppe native', async () => {
  const result = await runConnectorForDeclaration({
    declaration: {
      id: 'synthetic',
      type: 'camion-citerne',
      declarantUserId: 'synthetic-user',
      files: [],
      points: [],
    },
    localFiles: [
      {
        id: 'missing',
        type: 'camion-citerne',
        filename: 'missing.xlsx',
        url: 'unused',
        localPath: '/nonexistent/synthetic-reunion-file.xlsx',
      },
    ],
  })
  assert.equal(result.data?.nativeTemporal, true)
  assert.deepEqual(result.data?.series, [])
  assert.equal(result.errors.length, 1)
})
void test('les utilitaires normalisent sans perte les décimales et acceptent les dates françaises', () => {
  assert.equal(reunionDecimal('1 234,000000000000001'), '1234.000000000000001')
  assert.equal(reunionDecimal('-1.25e-15'), '-0.00000000000000125')
  assert.equal(reunionDecimal('0'), '0')
  assert.equal(reunionDecimal('-0,00'), '0')
  assert.equal(reunionDecimal('1,2,3'), undefined)
  assert.equal(reunionDate('lundi 12 février 2024'), '2024-02-12')
  assert.equal(reunionDate('31/02/2024'), undefined)
  assert.equal(reunionDate(1, true), '1904-01-02')
  assert.equal(reunionTime(0), '00:00:00.000')
  assert.equal(reunionTime(45846.5), '12:00:00.000')
  assert.equal(reunionTime('24:00'), undefined)
  assert.equal(reunionFrequency('1 année'), '1 year')
  assert.equal(reunionFrequency('15 min'), '15 minutes')
})
