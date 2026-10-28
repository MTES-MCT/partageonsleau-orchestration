import {createHash} from 'node:crypto'
import {readFile} from 'node:fs/promises'
import path from 'node:path'
import type {WorkBook, WorkSheet} from 'xlsx'
import {BaseConnector} from './base-connector.js'
import {
  SourceType,
  type ConnectorDiscoveryContext,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'
import type {
  NativeSeries,
  NativeSeriesValue,
  ReunionParseError,
  ReunionParseResult,
} from './reunion-types.js'
import {
  reunionParameter,
  reunionParameters,
  reunionUnit,
} from './reunion-parameters.js'
import {
  normalizeReunionText,
  reunionCamionPeriod,
  reunionDate,
  reunionDecimal,
  reunionFrequency,
  reunionPeriod,
  reunionTime,
} from './reunion-values.js'
import XLSX from './xlsx.js'

export type ReunionFileInput = {
  buffer: Buffer
  filename: string
  fileId?: string
  typeCode: string
}
export const REUNION_PARSER_VERSION = '1.0.2'
const CAMION_TYPES = [
  'camion-citerne',
  'reunion-camion-citerne',
  'reunion_camion_citerne',
]
const MULTI_TYPES = [
  'aep-zre',
  'icpe-hors-zre',
  'reunion-multi-param',
  'reunion_multi_param',
]
export function reunionConnectorName(type: string): string | undefined {
  if (CAMION_TYPES.includes(type)) return 'reunion_camion_citerne'
  if (MULTI_TYPES.includes(type)) return 'reunion_multi_param'
  return undefined
}

const camionPoints: Record<string, string> = {
  '412': 'Riv. St Denis La Colline',
  '413': 'Rav. à Jacques (La Montagne)',
  '414': 'Rav. Charpentier',
  '416': 'Ruisseau Emmanuel',
  '417': 'Petite riv St Jean',
  '418': 'Riv. Bras Panon',
  '419': 'Riv. des Galets',
  '420': 'Rav. Bernica',
  '421': 'Bras de la Plaine',
  '422': 'Riv. Des Remparts',
  '423': 'Source des Allemands',
}

function cellValue(sheet: WorkSheet, row: number, column: number): unknown {
  const cell = sheet[XLSX.utils.encode_cell({r: row - 1, c: column - 1})] as
    {v?: unknown} | undefined
  return cell?.v
}
function text(value: unknown): string {
  if (value instanceof Date) return value.toISOString()
  return typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
    ? String(value).trim()
    : ''
}
function isBlank(value: unknown): boolean {
  return text(value) === ''
}
function sourcePoint(value: unknown): string | undefined {
  const match = /^(\d+)(?:\s*(?:[|-]|\s)\s*.*)?$/.exec(text(value))
  return match
    ? `reunion:DEP-974:point:${BigInt(match[1]).toString()}`
    : undefined
}
function seriesKey(
  input: ReunionFileInput,
  sheet: string,
  column: number,
): string {
  return createHash('sha256')
    .update(JSON.stringify([input.fileId ?? input.filename, sheet, column]))
    .digest('hex')
}

function makeIssue(
  input: ReunionFileInput,
  context: Partial<ReunionParseError>,
  code: string,
  message: string,
): ReunionParseError {
  return {
    filename: input.filename,
    ...(input.fileId && {fileId: input.fileId}),
    severity: 'error',
    code,
    message,
    ...context,
  }
}
function atCell(
  row: number,
  column: number,
): {row: number; column: number; cell: string} {
  return {
    row,
    column,
    cell: XLSX.utils.encode_cell({r: row - 1, c: column - 1}),
  }
}

/** Keep identical duplicates once. Contradictory dates are entirely withheld for instruction. */
function deduplicate(
  values: NativeSeriesValue[],
  input: ReunionFileInput,
  context: Partial<ReunionParseError>,
  errors: ReunionParseError[],
): NativeSeriesValue[] {
  const byPeriod = new Map<string, NativeSeriesValue[]>()
  for (const value of values) {
    const key = `${value.periodStart}/${value.periodEnd}`
    const grouped = byPeriod.get(key)
    if (grouped) grouped.push(value)
    else byPeriod.set(key, [value])
  }
  const result: NativeSeriesValue[] = []
  for (const grouped of byPeriod.values()) {
    if (grouped.length === 1) {
      result.push(grouped[0])
      continue
    }
    if (new Set(grouped.map((value) => value.value)).size === 1) {
      result.push({
        ...grouped[0],
        metadata: {...grouped[0].metadata, duplicates: grouped.slice(1)},
      })
      errors.push(
        makeIssue(
          input,
          {
            ...context,
            severity: 'warning',
            row: grouped[1].metadata.row,
            cell: grouped[1].metadata.cell,
          },
          'DUPLICATE_IDENTICAL',
          'Valeur identique répétée : conservée une seule fois.',
        ),
      )
    } else {
      for (const value of grouped)
        errors.push(
          makeIssue(
            input,
            {
              ...context,
              row: value.metadata.row,
              cell: value.metadata.cell,
              rawValue: value.value,
            },
            'DUPLICATE_CONFLICT',
            'Plusieurs valeurs différentes pour la même date : valeurs à instruire.',
          ),
        )
    }
  }
  return result.sort((a, b) => a.periodStart.localeCompare(b.periodStart))
}

function finishSeries(
  input: ReunionFileInput,
  series: NativeSeries,
  errors: ReunionParseError[],
): void {
  series.data = deduplicate(
    series.data,
    input,
    {sheet: text(series.metadata.sheet), seriesKey: series.seriesKey},
    errors,
  )
  series.minDate = series.data[0]?.periodStart ?? ''
  series.maxDate = series.data.reduce(
    (latest, value) => (value.periodEnd > latest ? value.periodEnd : latest),
    '',
  )
}
function newSeries(
  input: ReunionFileInput,
  sheet: string,
  column: number,
  point: string,
  parameter: string,
  unit: string,
  frequency: string,
  valueType: string,
): NativeSeries {
  return {
    nativeTemporal: true,
    seriesKey: seriesKey(input, sheet, column),
    pointPrelevement: point,
    parameter,
    unit,
    frequency,
    valueType,
    autoCalculateVolumes: false,
    minDate: '',
    maxDate: '',
    data: [],
    metadata: {
      timezone: 'Indian/Reunion',
      parser: reunionConnectorName(input.typeCode),
      parserVersion: REUNION_PARSER_VERSION,
      sourceFile: input.filename,
      ...(input.fileId && {fileId: input.fileId}),
      sheet,
      column,
    },
  }
}

function parseCamion(
  input: ReunionFileInput,
  workbook: WorkBook,
): ReunionParseResult {
  const result: ReunionParseResult = {series: [], errors: []}
  const sheetName = workbook.SheetNames[0]
  const sheet = workbook.Sheets[sheetName]
  if (!sheet?.['!ref']) {
    result.errors.push(
      makeIssue(
        input,
        {},
        'EMPTY_FILE',
        'Le fichier ne contient pas de données.',
      ),
    )
    return result
  }
  const range = XLSX.utils.decode_range(sheet['!ref'])
  if (normalizeReunionText(text(cellValue(sheet, 3, 1))) !== 'date') {
    result.errors.push(
      makeIssue(
        input,
        {sheet: sheetName, ...atCell(3, 1)},
        'INVALID_DATE_HEADER',
        'La colonne A3 doit contenir Date.',
      ),
    )
    return result
  }
  const pointColumns = new Map<string, number[]>()
  for (let col = 2; col <= range.e.c + 1; col++) {
    const header = text(cellValue(sheet, 3, col)).replaceAll(/\s+/g, ' ')
    if (!header) continue
    const match = /^(\d+)\s+(.*)$/.exec(header)
    if (
      !match ||
      !camionPoints[match[1]] ||
      normalizeReunionText(match[2]) !==
        normalizeReunionText(camionPoints[match[1]])
    ) {
      result.errors.push(
        makeIssue(
          input,
          {sheet: sheetName, ...atCell(3, col), rawValue: header},
          'INVALID_POINT_HEADER',
          'Le code ou le nom du point ne correspond pas au modèle camion-citerne.',
        ),
      )
      continue
    }
    pointColumns.set(match[1], [...(pointColumns.get(match[1]) ?? []), col])
  }
  for (const [point, columns] of pointColumns) {
    if (columns.length > 1) {
      for (const column of columns)
        result.errors.push(
          makeIssue(
            input,
            {sheet: sheetName, ...atCell(3, column)},
            'DUPLICATE_POINT_COLUMN',
            'Plusieurs colonnes pour le même point : colonnes à instruire.',
          ),
        )
      continue
    }
    const column = columns[0]
    const series = newSeries(
      input,
      sheetName,
      column,
      `reunion:DEP-974:point:${point}`,
      'volume prélevé',
      'm³',
      '1 day',
      'cumulative',
    )
    for (let row = 4; row <= range.e.r + 1; row++) {
      const rawValue = cellValue(sheet, row, column)
      if (isBlank(rawValue)) continue
      const context = {
        sheet: sheetName,
        seriesKey: series.seriesKey,
        ...atCell(row, column),
        rawValue: text(rawValue),
      }
      const value = reunionDecimal(rawValue)
      const period = reunionCamionPeriod(
        cellValue(sheet, row, 1),
        workbook.Workbook?.WBProps?.date1904 === true,
      )
      if (!period) {
        result.errors.push(
          makeIssue(
            input,
            context,
            'INVALID_DATE',
            'Date absente ou invalide.',
          ),
        )
        continue
      }
      if (value === undefined || Number(value) < 0) {
        result.errors.push(
          makeIssue(
            input,
            context,
            'INVALID_VALUE',
            'Le volume doit être un nombre positif ou nul.',
          ),
        )
        continue
      }
      if (period.frequency === 'irregular') series.frequency = 'irregular'
      series.data.push({
        date: period.date,
        periodStart: period.periodStart,
        periodEnd: period.periodEnd,
        value,
        metadata: {
          row,
          cell: context.cell,
          rawValue: text(rawValue),
          rawDate: text(cellValue(sheet, row, 1)),
        },
      })
    }
    finishSeries(input, series, result.errors)
    if (series.data.length) result.series.push(series)
  }
  if (!result.series.length && !result.errors.length)
    result.errors.push(
      makeIssue(input, {}, 'NO_DATA', 'Le fichier ne contient aucune valeur.'),
    )
  return result
}

const valueTypes: Record<string, string> = {
  'valeur brute': 'instantaneous',
  moyenne: 'average',
  minimum: 'minimum',
  maximum: 'maximum',
  mediane: 'median',
  "difference d'index": 'delta-index',
  autre: 'raw',
}
const subDaily = new Set([
  'instant',
  '1 second',
  '1 minute',
  '15 minutes',
  '1 hour',
  '6 hours',
])

function parseMulti(
  input: ReunionFileInput,
  workbook: WorkBook,
): ReunionParseResult {
  const result: ReunionParseResult = {series: [], errors: []}
  const metadataName = workbook.SheetNames.find(
    (name) => normalizeReunionText(name).replaceAll('_', ' ') === 'a lire',
  )
  const rawPoint = metadataName
    ? cellValue(workbook.Sheets[metadataName], 3, 2)
    : undefined
  const point = sourcePoint(rawPoint)
  if (!point)
    result.errors.push(
      makeIssue(
        input,
        {
          sheet: metadataName ?? 'A LIRE',
          ...atCell(3, 2),
          rawValue: text(rawPoint),
        },
        'UNRESOLVED_POINT',
        'Code du point absent ou invalide : association nécessaire.',
      ),
    )
  const sheetNames = workbook.SheetNames.filter((name) =>
    /^data\s*\|\s*t\s*=/i.test(normalizeReunionText(name).replaceAll('_', ' ')),
  )
  if (!sheetNames.length)
    result.errors.push(
      makeIssue(
        input,
        {},
        'NO_DATA_SHEET',
        'Aucun onglet Data | T=... trouvé.',
      ),
    )
  const date1904 = workbook.Workbook?.WBProps?.date1904 === true
  for (const sheetName of sheetNames) {
    const sheet = workbook.Sheets[sheetName]
    const range = XLSX.utils.decode_range(sheet['!ref'] ?? 'A1')
    if (
      normalizeReunionText(text(cellValue(sheet, 12, 1))) !== 'date' ||
      normalizeReunionText(text(cellValue(sheet, 12, 2))) !== 'heure'
    ) {
      result.errors.push(
        makeIssue(
          input,
          {sheet: sheetName, row: 12},
          'INVALID_DATA_HEADER',
          'Les colonnes A12 et B12 doivent contenir Date et Heure.',
        ),
      )
      continue
    }
    let remarkColumn: number | undefined
    for (let column = 3; column <= range.e.c + 1; column++)
      if (
        normalizeReunionText(text(cellValue(sheet, 12, column))) === 'remarque'
      )
        remarkColumn = column
    if (!remarkColumn)
      result.errors.push(
        makeIssue(
          input,
          {sheet: sheetName, row: 12, severity: 'warning'},
          'MISSING_REMARK_COLUMN',
          'Colonne Remarque absente.',
        ),
      )
    for (let column = 3; column <= 9; column++) {
      if (column === remarkColumn) continue
      let hasValues = false
      for (let row = 13; row <= range.e.r + 1; row++) {
        if (!isBlank(cellValue(sheet, row, column))) {
          hasValues = true
          break
        }
      }
      // The legacy template has prefilled metadata for unused columns.
      if (!hasValues) continue
      const originalParameter = text(cellValue(sheet, 2, column))
      if (!originalParameter) {
        if (
          Array.from({length: Math.max(0, range.e.r - 11)}, (_, offset) =>
            cellValue(sheet, 13 + offset, column),
          ).some((value) => !isBlank(value))
        )
          result.errors.push(
            makeIssue(
              input,
              {sheet: sheetName, ...atCell(2, column)},
              'MISSING_PARAMETER',
              'Une colonne de valeurs ne précise pas son paramètre.',
            ),
          )
        continue
      }
      const parameter = reunionParameter(originalParameter)
      const unit = reunionUnit(text(cellValue(sheet, 5, column)))
      const originalValueType = text(cellValue(sheet, 3, column))
      const originalType = valueTypes[normalizeReunionText(originalValueType)]
      const cumulative = ['volume prélevé', 'volume restitué'].includes(
        parameter ?? '',
      )
      const rawFrequency = text(cellValue(sheet, 4, column))
      // An instantaneous measurement has no inferred duration. Unknown
      // frequencies remain invalid for volumes and aggregated measurements.
      const frequency =
        reunionFrequency(rawFrequency) ??
        (normalizeReunionText(rawFrequency) === 'autre' &&
        originalType === 'instantaneous' &&
        !cumulative
          ? 'irregular'
          : undefined)
      const context = {
        sheet: sheetName,
        column,
        seriesKey: seriesKey(input, sheetName, column),
      }
      if (
        !normalizeReunionText(text(cellValue(sheet, 12, column))).startsWith(
          'valeur_parametre',
        )
      ) {
        result.errors.push(
          makeIssue(
            input,
            {...context, ...atCell(12, column)},
            'INVALID_VALUE_HEADER',
            'En-tête de valeur modifié.',
          ),
        )
        continue
      }
      if (!parameter) {
        result.errors.push(
          makeIssue(
            input,
            {...context, ...atCell(2, column), rawValue: originalParameter},
            'UNKNOWN_PARAMETER',
            'Paramètre non reconnu.',
          ),
        )
        continue
      }
      if (!unit || !reunionParameters[parameter].units[unit]) {
        result.errors.push(
          makeIssue(
            input,
            {
              ...context,
              ...atCell(5, column),
              rawValue: text(cellValue(sheet, 5, column)),
            },
            'INVALID_UNIT',
            'Unité absente ou incompatible avec le paramètre.',
          ),
        )
        continue
      }
      if (!frequency) {
        result.errors.push(
          makeIssue(
            input,
            {
              ...context,
              ...atCell(4, column),
              rawValue: text(cellValue(sheet, 4, column)),
            },
            'INVALID_FREQUENCY',
            'Fréquence absente ou non reconnue.',
          ),
        )
        continue
      }
      if (!originalType) {
        result.errors.push(
          makeIssue(
            input,
            {...context, ...atCell(3, column), rawValue: originalValueType},
            'INVALID_VALUE_TYPE',
            'Nature de la valeur absente ou non reconnue.',
          ),
        )
        continue
      }
      const declaredSheetFrequency = reunionFrequency(
        sheetName.split('=').slice(1).join('='),
      )
      if (declaredSheetFrequency && declaredSheetFrequency !== frequency)
        result.errors.push(
          makeIssue(
            input,
            {...context, ...atCell(4, column), severity: 'warning'},
            'FREQUENCY_MISMATCH',
            'La fréquence renseignée diffère du titre de l’onglet ; la fréquence renseignée est conservée.',
          ),
        )
      if (cumulative && frequency === 'instant') {
        result.errors.push(
          makeIssue(
            input,
            context,
            'INVALID_VOLUME_PERIOD',
            'Le volume nécessite une période.',
          ),
        )
        continue
      }
      const valueType = cumulative ? 'cumulative' : originalType
      const series = newSeries(
        input,
        sheetName,
        column,
        point ?? text(rawPoint),
        parameter,
        unit,
        frequency,
        valueType,
      )
      const startRaw = cellValue(sheet, 8, column),
        endRaw = cellValue(sheet, 9, column)
      const start = reunionDate(startRaw, date1904),
        end = reunionDate(endRaw, date1904)
      const invalidDateRange =
        (!isBlank(startRaw) && !start) ||
        (!isBlank(endRaw) && !end) ||
        (start && end && start > end)
      if (invalidDateRange)
        result.errors.push(
          makeIssue(
            input,
            {...context, severity: 'warning'},
            'INVALID_METADATA_DATES',
            'Dates de début ou de fin invalides ; les dates explicites des mesures sont conservées.',
          ),
        )
      const depthRaw = cellValue(sheet, 7, column),
        depth = reunionDecimal(depthRaw)
      if (!isBlank(depthRaw) && depth === undefined)
        result.errors.push(
          makeIssue(
            input,
            {
              ...context,
              ...atCell(7, column),
              rawValue: text(depthRaw),
              severity: 'warning',
            },
            'INVALID_DEPTH',
            'Profondeur invalide, conservée dans les métadonnées.',
          ),
        )
      series.metadata = {
        ...series.metadata,
        originalParameter,
        originalValueType,
        originalPoint: text(rawPoint),
        detailPointSuivi: text(cellValue(sheet, 6, column)),
        profondeur: depth ?? text(depthRaw),
        commentaire: text(cellValue(sheet, 10, column)),
        dateDebut: text(startRaw),
        dateFin: text(endRaw),
      }
      for (let row = 13; row <= range.e.r + 1; row++) {
        const rawValue = cellValue(sheet, row, column)
        if (isBlank(rawValue)) continue
        const cellContext = {
          ...context,
          ...atCell(row, column),
          rawValue: text(rawValue),
        }
        const value = reunionDecimal(rawValue)
        const bounds = reunionParameters[parameter].units[unit]
        if (
          value === undefined ||
          (bounds.min !== undefined && Number(value) < bounds.min) ||
          (bounds.max !== undefined && Number(value) > bounds.max)
        ) {
          result.errors.push(
            makeIssue(
              input,
              cellContext,
              'INVALID_VALUE',
              'Valeur non numérique ou hors des bornes du paramètre.',
            ),
          )
          continue
        }
        const day = reunionDate(cellValue(sheet, row, 1), date1904)
        if (!day) {
          result.errors.push(
            makeIssue(
              input,
              cellContext,
              'INVALID_DATE',
              'Date absente ou invalide.',
            ),
          )
          continue
        }
        if (
          !invalidDateRange &&
          ((start && day < start) || (end && day > end))
        ) {
          result.errors.push(
            makeIssue(
              input,
              cellContext,
              'DATE_OUTSIDE_METADATA',
              'Date hors de la période déclarée.',
            ),
          )
          continue
        }
        const rawTime = cellValue(sheet, row, 2)
        const time = reunionTime(rawTime)
        if (
          ((subDaily.has(frequency) || frequency === 'irregular') && !time) ||
          (!isBlank(rawTime) && !time)
        ) {
          result.errors.push(
            makeIssue(
              input,
              cellContext,
              'INVALID_TIME',
              'Heure absente ou invalide.',
            ),
          )
          continue
        }
        const remark = remarkColumn
          ? text(cellValue(sheet, row, remarkColumn))
          : ''
        const punctual =
          !cumulative &&
          (originalType === 'instantaneous' ||
            parameter === 'relevé d’index de compteur' ||
            frequency === 'instant')
        series.data.push({
          ...reunionPeriod(day, time ?? '00:00:00.000', frequency, punctual),
          value,
          ...(remark && {remark}),
          metadata: {
            row,
            cell: cellContext.cell,
            rawValue: text(rawValue),
            rawDate: text(cellValue(sheet, row, 1)),
            rawTime: text(rawTime),
          },
        })
      }
      finishSeries(input, series, result.errors)
      if (series.data.length) result.series.push(series)
    }
  }
  if (!result.series.length && !result.errors.length)
    result.errors.push(
      makeIssue(input, {}, 'NO_DATA', 'Le fichier ne contient aucune valeur.'),
    )
  return result
}

/** Pure parser, usable by the historical import without starting workers or network clients. */
export function parseReunionFile(input: ReunionFileInput): ReunionParseResult {
  if (!reunionConnectorName(input.typeCode))
    return {
      series: [],
      errors: [
        makeIssue(
          input,
          {},
          'UNSUPPORTED_FORMAT',
          'Format de déclaration Réunion non reconnu.',
        ),
      ],
    }
  if (!/\.(xlsx?|ods)$/i.test(input.filename))
    return {
      series: [],
      errors: [
        makeIssue(
          input,
          {severity: 'warning'},
          'ATTACHMENT_ONLY',
          'Pièce jointe conservée, sans analyse de données tabulaires.',
        ),
      ],
    }
  let workbook: WorkBook
  try {
    workbook = XLSX.read(input.buffer, {
      type: 'buffer',
      cellDates: false,
      cellFormula: true,
    })
  } catch {
    return {
      series: [],
      errors: [
        makeIssue(
          input,
          {},
          'UNREADABLE_WORKBOOK',
          'Le classeur ne peut pas être lu.',
        ),
      ],
    }
  }
  return CAMION_TYPES.includes(input.typeCode)
    ? parseCamion(input, workbook)
    : parseMulti(input, workbook)
}

/** Registry integration keeps native decimal series outside legacy numeric metrics. */
export class ReunionConnector extends BaseConnector<
  ReunionFileInput,
  ReunionParseResult
> {
  constructor(name: 'reunion_camion_citerne' | 'reunion_multi_param') {
    super(name)
  }
  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<ReunionFileInput> {
    if (!context.sourceFile) throw new Error('Fichier Réunion manquant.')
    return {
      buffer: await readFile(context.sourceFile),
      filename:
        context.sourceFiles?.[0]?.filename ?? path.basename(context.sourceFile),
      typeCode: this.name,
    }
  }
  protected async parse(input: ReunionFileInput): Promise<ReunionParseResult> {
    return parseReunionFile(input)
  }
  protected async process(
    result: ReunionParseResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const nativeSeries = result.series.filter(
      (series) => series.pointPrelevement === context.sourcePointId,
    )
    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.DECLARATION,
      source_metadata: {
        parser: this.name,
        parserVersion: REUNION_PARSER_VERSION,
      },
      min_date: undefined,
      max_date: undefined,
      metrics: [],
      nativeSeries,
      parsingErrors: result.errors,
    }
  }
  async discoverSourcePointIds(
    context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    if (!context.sourceFile) return []
    const result = parseReunionFile({
      buffer: await readFile(context.sourceFile),
      filename:
        context.sourceFiles?.[0]?.filename ?? path.basename(context.sourceFile),
      typeCode: this.name,
    })
    return [...new Set(result.series.map((series) => series.pointPrelevement))]
  }
}
