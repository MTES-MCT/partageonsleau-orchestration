import path from 'node:path'
import moment from 'moment'
import xlsx, {type Range, type WorkSheet} from 'xlsx'
import {BaseConnector} from './base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorDiscoveryContext,
  type ConnectorRunContext,
  type ConnectorSourceFile,
  type ParsedPointPayload,
  type TimeserieValue,
} from './types.js'

const CONNECTOR_NAME = 'gidaf'
const CADRES_FILE_TYPE = 'gidaf-cadres'
const PRELEVEMENTS_FILE_TYPE = 'gidaf-prelevements'

type ColumnDefinition<TKey extends string> = {
  key: TKey
  matchers: string[]
}

type CadresColumnKey = 'codeInspection' | 'pointSurveillance' | 'typePoint'

type PrelevementsColumnKey =
  | 'codeInspection'
  | 'pointSurveillance'
  | 'typePoint'
  | 'dateMesure'
  | 'volume'

type GidafCadreRow = {
  codeInspection: string | undefined
  pointSurveillance: string
  typePoint: string | undefined
  sourcePointId: string | undefined
}

type GidafMetricType = MetricType.VOLUME_PRELEVE | MetricType.VOLUME_REJETE

type GidafRawRow = {
  sourcePointId: string | undefined
  pointSurveillance: string
  metricType: GidafMetricType
  dateStart: Date
  dateEnd: Date
  value: number
}

type GidafSelectedFiles = {
  cadres: ConnectorSourceFile
  prelevements: ConnectorSourceFile
}

type GidafFetchResult = {
  files: GidafSelectedFiles
  records: GidafRawRow[]
}

type GidafParsedResult = GidafFetchResult

type XlsxCell = {
  value: unknown
}

type ParsedExcelDateCode = {
  y: number
  m: number
  d: number
  H: number
  M: number
  S: number
}

const CADRES_COLUMNS: Array<ColumnDefinition<CadresColumnKey>> = [
  {key: 'codeInspection', matchers: ['code_inspection']},
  {key: 'pointSurveillance', matchers: ['point_de_surveillance']},
  {key: 'typePoint', matchers: ['type_de_point']},
]

const PRELEVEMENTS_COLUMNS: Array<ColumnDefinition<PrelevementsColumnKey>> = [
  {key: 'codeInspection', matchers: ['code_inspection']},
  {key: 'pointSurveillance', matchers: ['point_de_surveillance']},
  {key: 'typePoint', matchers: ['type_de_point']},
  {key: 'dateMesure', matchers: ['date_de_mesure']},
  {key: 'volume', matchers: ['volume_(m3)', 'volume_m3', 'volume']},
]

const CADRES_REQUIRED_HEADERS = ['code_inspection', 'point_de_surveillance']
const PRELEVEMENTS_REQUIRED_HEADERS = [
  'code_inspection',
  'point_de_surveillance',
  'date_de_mesure',
  'volume',
]

const GIDAF_DATE_FORMATS = [
  'YYYY-MM-DD',
  'DD/MM/YYYY',
  'D/M/YYYY',
  'DD-MM-YYYY',
  'D-M-YYYY',
  'DD/MM/YYYY HH:mm:ss',
  'DD/MM/YYYY HH:mm',
  'YYYY-MM-DD HH:mm:ss',
  'YYYY-MM-DD HH:mm',
]

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isParseDateCodeFunction(
  value: unknown,
): value is (serialDate: number) => unknown {
  return typeof value === 'function'
}

function toParsedExcelDateCode(
  value: unknown,
): ParsedExcelDateCode | undefined {
  if (!isRecord(value)) {
    return undefined
  }

  const {y, m, d, H, M, S} = value

  if (
    typeof y !== 'number' ||
    typeof m !== 'number' ||
    typeof d !== 'number' ||
    typeof H !== 'number' ||
    typeof M !== 'number' ||
    typeof S !== 'number'
  ) {
    return undefined
  }

  return {y, m, d, H, M, S}
}

function scalarToString(value: unknown): string | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? trimmed : undefined
  }

  if (
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'bigint'
  ) {
    return String(value)
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString()
  }

  return undefined
}

function stripDiacritics(value: unknown): string {
  return (scalarToString(value) ?? '')
    .normalize('NFD')
    .replaceAll(/[\u0300-\u036F]/gv, '')
}

function normalizeHeader(value: unknown): string {
  return stripDiacritics(value).toLowerCase().trim().replaceAll(/\s+/gv, '_')
}

function normalizeSpaces(value: unknown): string {
  return (scalarToString(value) ?? '').replaceAll(/\s+/gv, ' ').trim()
}

function normalizeSourcePart(value: unknown): string {
  return stripDiacritics(normalizeSpaces(value))
    .replaceAll(/[^a-zA-Z\d]+/gv, '-')
    .replaceAll(/-+/gv, '-')
    .replaceAll(/^-|-$/gv, '')
    .toLowerCase()
}

function normalizeIdentifier(value: string): string {
  return stripDiacritics(value)
    .toLowerCase()
    .replaceAll(/[^a-z0-9]/gv, '')
}

function readCell(
  sheet: WorkSheet,
  rowIndex: number,
  columnIndex: number,
): XlsxCell | undefined {
  const value: unknown =
    sheet[xlsx.utils.encode_cell({c: columnIndex, r: rowIndex})]

  if (!isRecord(value)) {
    return undefined
  }

  return {
    value: value.v,
  }
}

function readAsString(
  sheet: WorkSheet,
  rowIndex: number,
  columnIndex: number,
): string | undefined {
  const cell = readCell(sheet, rowIndex, columnIndex)

  return cell ? scalarToString(cell.value) : undefined
}

function readAsNumber(
  sheet: WorkSheet,
  rowIndex: number,
  columnIndex: number,
): number | undefined {
  const cell = readCell(sheet, rowIndex, columnIndex)

  if (cell?.value === undefined || cell.value === null || cell.value === '') {
    return undefined
  }

  if (typeof cell.value === 'number' && Number.isFinite(cell.value)) {
    return cell.value
  }

  if (typeof cell.value !== 'string') {
    return undefined
  }

  const normalized = cell.value
    .trim()
    .replaceAll(/[\s\u00A0\u202F]+/gv, '')
    .replace(',', '.')
  const value = Number(normalized)

  return Number.isFinite(value) ? value : undefined
}

function dateFromExcelSerial(value: number): Date | undefined {
  const ssf: unknown = xlsx.SSF

  if (!isRecord(ssf) || !isParseDateCodeFunction(ssf.parse_date_code)) {
    return undefined
  }

  const parsed = toParsedExcelDateCode(ssf.parse_date_code(value))

  if (!parsed) {
    return undefined
  }

  return new Date(
    Date.UTC(parsed.y, parsed.m - 1, parsed.d, parsed.H, parsed.M, parsed.S),
  )
}

function readAsDate(
  sheet: WorkSheet,
  rowIndex: number,
  columnIndex: number,
): Date | undefined {
  const cell = readCell(sheet, rowIndex, columnIndex)

  if (cell?.value === undefined || cell.value === null || cell.value === '') {
    return undefined
  }

  if (cell.value instanceof Date && !Number.isNaN(cell.value.getTime())) {
    return cell.value
  }

  if (typeof cell.value === 'number' && Number.isFinite(cell.value)) {
    return dateFromExcelSerial(cell.value)
  }

  const text = scalarToString(cell.value)

  if (!text) {
    return undefined
  }

  const parsedDate = moment.utc(text, GIDAF_DATE_FORMATS, true)

  if (parsedDate.isValid()) {
    return parsedDate.toDate()
  }

  const parsedIsoDate = moment.utc(text, moment.ISO_8601, true)
  return parsedIsoDate.isValid() ? parsedIsoDate.toDate() : undefined
}

function startOfMonth(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1))
}

function isPrelevementType(typePoint: string | undefined): boolean {
  return stripDiacritics(typePoint).toLowerCase().includes('alimentation')
}

function buildGidafPointSourceId(parameters: {
  codeInspection: string | undefined
  pointSurveillance: string | undefined
  typePoint: string | undefined
}): string | undefined {
  const {codeInspection, pointSurveillance, typePoint} = parameters

  if (!codeInspection || !pointSurveillance) {
    return undefined
  }

  return [
    'blv',
    'industriels-icpe-gidaf',
    normalizeSourcePart(codeInspection),
    normalizeSourcePart(pointSurveillance),
    normalizeSourcePart(typePoint ?? 'unknown'),
  ].join('-')
}

function getSheetRange(sheet: WorkSheet): Range | undefined {
  const reference = sheet['!ref']

  if (!reference) {
    return undefined
  }

  return xlsx.utils.decode_range(reference)
}

function findHeaderRow(
  sheet: WorkSheet,
  requiredHeaders: string[],
): number | undefined {
  const range = getSheetRange(sheet)

  if (!range) {
    return undefined
  }

  for (let row = 0; row <= Math.min(10, range.e.r); row += 1) {
    const rowValues: string[] = []

    for (let column = 0; column <= range.e.c; column += 1) {
      rowValues.push(normalizeHeader(readAsString(sheet, row, column)))
    }

    const hasAllRequiredHeaders = requiredHeaders.every((requiredHeader) =>
      rowValues.some(
        (value) => value === requiredHeader || value.includes(requiredHeader),
      ),
    )

    if (hasAllRequiredHeaders) {
      return row
    }
  }

  return undefined
}

function mapColumns<TKey extends string>(
  sheet: WorkSheet,
  headerRow: number,
  columnDefinitions: Array<ColumnDefinition<TKey>>,
): Partial<Record<TKey, number>> {
  const range = getSheetRange(sheet)
  const columnMap: Partial<Record<TKey, number>> = {}

  if (!range) {
    return columnMap
  }

  for (let column = 0; column <= range.e.c; column += 1) {
    const headerValue = normalizeHeader(readAsString(sheet, headerRow, column))

    for (const definition of columnDefinitions) {
      if (columnMap[definition.key] !== undefined) {
        continue
      }

      if (
        definition.matchers.some(
          (matcher) => headerValue === matcher || headerValue.includes(matcher),
        )
      ) {
        columnMap[definition.key] = column
      }
    }
  }

  return columnMap
}

function readMappedString<TKey extends string>(
  sheet: WorkSheet,
  rowIndex: number,
  columnMap: Partial<Record<TKey, number>>,
  key: TKey,
): string | undefined {
  const columnIndex = columnMap[key]

  return columnIndex === undefined
    ? undefined
    : readAsString(sheet, rowIndex, columnIndex)
}

function readMappedNumber<TKey extends string>(
  sheet: WorkSheet,
  rowIndex: number,
  columnMap: Partial<Record<TKey, number>>,
  key: TKey,
): number | undefined {
  const columnIndex = columnMap[key]

  return columnIndex === undefined
    ? undefined
    : readAsNumber(sheet, rowIndex, columnIndex)
}

function readMappedDate<TKey extends string>(
  sheet: WorkSheet,
  rowIndex: number,
  columnMap: Partial<Record<TKey, number>>,
  key: TKey,
): Date | undefined {
  const columnIndex = columnMap[key]

  return columnIndex === undefined
    ? undefined
    : readAsDate(sheet, rowIndex, columnIndex)
}

function readFirstWorksheet(filePath: string): WorkSheet | undefined {
  const workbook = xlsx.readFile(filePath)
  const sheetName = workbook.SheetNames[0]

  return sheetName ? workbook.Sheets[sheetName] : undefined
}

function fileHasHeaders(filePath: string, requiredHeaders: string[]): boolean {
  try {
    const sheet = readFirstWorksheet(filePath)
    return sheet ? findHeaderRow(sheet, requiredHeaders) !== undefined : false
  } catch {
    return false
  }
}

function isCadresFilename(filename: string): boolean {
  return normalizeIdentifier(filename).includes('cadres')
}

function isPrelevementsFilename(filename: string): boolean {
  const normalized = normalizeIdentifier(filename)
  return (
    normalized.includes('prelevement') || normalized.includes('prelevements')
  )
}

function normalizeContextSourceFile(sourceFile: string): ConnectorSourceFile {
  return {
    type: 'gidaf',
    filename: path.basename(sourceFile),
    path: sourceFile,
  }
}

function selectGidafFiles(
  context: ConnectorRunContext | ConnectorDiscoveryContext,
): GidafSelectedFiles {
  const sourceFiles =
    context.sourceFiles ??
    (context.sourceFile ? [normalizeContextSourceFile(context.sourceFile)] : [])

  const cadres =
    sourceFiles.find((file) => file.type === CADRES_FILE_TYPE) ??
    sourceFiles.find((file) => isCadresFilename(file.filename)) ??
    sourceFiles.find((file) =>
      fileHasHeaders(file.path, CADRES_REQUIRED_HEADERS),
    )

  const prelevements =
    sourceFiles.find((file) => file.type === PRELEVEMENTS_FILE_TYPE) ??
    sourceFiles.find((file) => isPrelevementsFilename(file.filename)) ??
    sourceFiles.find((file) =>
      fileHasHeaders(file.path, PRELEVEMENTS_REQUIRED_HEADERS),
    )

  if (!cadres || !prelevements) {
    throw new Error(
      `[${CONNECTOR_NAME}] Les fichiers "Cadres" et "Prelevements" sont obligatoires.`,
    )
  }

  if (cadres.path === prelevements.path) {
    throw new Error(
      `[${CONNECTOR_NAME}] Les fichiers "Cadres" et "Prelevements" doivent être distincts.`,
    )
  }

  return {cadres, prelevements}
}

function extractCadresRows(sheet: WorkSheet): GidafCadreRow[] {
  const range = getSheetRange(sheet)
  const headerRow = findHeaderRow(sheet, CADRES_REQUIRED_HEADERS)

  if (!range || headerRow === undefined) {
    return []
  }

  const columnMap = mapColumns(sheet, headerRow, CADRES_COLUMNS)
  const rows: GidafCadreRow[] = []

  for (let rowIndex = headerRow + 1; rowIndex <= range.e.r; rowIndex += 1) {
    const pointSurveillance = readMappedString(
      sheet,
      rowIndex,
      columnMap,
      'pointSurveillance',
    )

    if (!pointSurveillance) {
      continue
    }

    const codeInspection = readMappedString(
      sheet,
      rowIndex,
      columnMap,
      'codeInspection',
    )
    const typePoint = readMappedString(sheet, rowIndex, columnMap, 'typePoint')

    rows.push({
      codeInspection,
      pointSurveillance,
      typePoint,
      sourcePointId: buildGidafPointSourceId({
        codeInspection,
        pointSurveillance,
        typePoint,
      }),
    })
  }

  return rows
}

function buildCadresByPointSurveillance(
  cadresRows: GidafCadreRow[],
): Map<string, GidafCadreRow> {
  const cadresByPointSurveillance = new Map<string, GidafCadreRow>()

  for (const row of cadresRows) {
    const key = normalizeIdentifier(row.pointSurveillance)

    if (!cadresByPointSurveillance.has(key)) {
      cadresByPointSurveillance.set(key, row)
    }
  }

  return cadresByPointSurveillance
}

function extractPrelevementRows(
  sheet: WorkSheet,
  cadresRows: GidafCadreRow[],
): GidafRawRow[] {
  const range = getSheetRange(sheet)
  const headerRow = findHeaderRow(sheet, PRELEVEMENTS_REQUIRED_HEADERS)

  if (!range || headerRow === undefined) {
    return []
  }

  const columnMap = mapColumns(sheet, headerRow, PRELEVEMENTS_COLUMNS)
  const cadresByPointSurveillance = buildCadresByPointSurveillance(cadresRows)
  const rows: GidafRawRow[] = []

  for (let rowIndex = headerRow + 1; rowIndex <= range.e.r; rowIndex += 1) {
    const pointSurveillance = readMappedString(
      sheet,
      rowIndex,
      columnMap,
      'pointSurveillance',
    )
    const dateEnd = readMappedDate(sheet, rowIndex, columnMap, 'dateMesure')
    const value = readMappedNumber(sheet, rowIndex, columnMap, 'volume')

    if (!pointSurveillance || !dateEnd || value === undefined || value < 0) {
      continue
    }

    const cadre = cadresByPointSurveillance.get(
      normalizeIdentifier(pointSurveillance),
    )
    const codeInspection =
      readMappedString(sheet, rowIndex, columnMap, 'codeInspection') ??
      cadre?.codeInspection
    const typePoint =
      readMappedString(sheet, rowIndex, columnMap, 'typePoint') ??
      cadre?.typePoint
    const metricType = isPrelevementType(typePoint)
      ? MetricType.VOLUME_PRELEVE
      : MetricType.VOLUME_REJETE

    rows.push({
      sourcePointId:
        buildGidafPointSourceId({
          codeInspection,
          pointSurveillance,
          typePoint,
        }) ?? cadre?.sourcePointId,
      pointSurveillance,
      metricType,
      dateStart: startOfMonth(dateEnd),
      dateEnd,
      value,
    })
  }

  return rows
}

function rowMatchesSourcePointId(
  row: GidafRawRow,
  sourcePointId: string,
): boolean {
  const normalizedSourcePointId = normalizeIdentifier(sourcePointId)
  const candidates = [row.sourcePointId, row.pointSurveillance]

  return candidates.some((candidate) => {
    if (!candidate) {
      return false
    }

    const normalizedCandidate = normalizeIdentifier(candidate)

    if (normalizedCandidate === normalizedSourcePointId) {
      return true
    }

    return (
      normalizedCandidate.length >= 4 &&
      normalizedSourcePointId.includes(normalizedCandidate)
    )
  })
}

function getDateBounds(records: GidafRawRow[]): {
  minDate: Date | undefined
  maxDate: Date | undefined
} {
  if (records.length === 0) {
    return {
      minDate: undefined,
      maxDate: undefined,
    }
  }

  return {
    minDate: new Date(
      Math.min(...records.map((record) => record.dateStart.getTime())),
    ),
    maxDate: new Date(
      Math.max(...records.map((record) => record.dateEnd.getTime())),
    ),
  }
}

function sortValuesByDate(values: TimeserieValue[]): TimeserieValue[] {
  return values.toSorted(
    (left, right) => left.date.getTime() - right.date.getTime(),
  )
}

export class GidafConnector extends BaseConnector<
  GidafFetchResult,
  GidafParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')

  constructor() {
    super(CONNECTOR_NAME)
  }

  async discoverSourcePointIds(
    context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    const files = selectGidafFiles(context)
    const cadresSheet = readFirstWorksheet(files.cadres.path)
    const prelevementsSheet = readFirstWorksheet(files.prelevements.path)

    if (!cadresSheet || !prelevementsSheet) {
      return []
    }

    const cadresRows = extractCadresRows(cadresSheet)
    const records = extractPrelevementRows(prelevementsSheet, cadresRows)
    const sourcePointIds = records.flatMap((record) => {
      const sourcePointId = record.sourcePointId ?? record.pointSurveillance

      return sourcePointId ? [sourcePointId] : []
    })

    return [...new Set(sourcePointIds)]
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<GidafFetchResult> {
    const files = selectGidafFiles(context)
    const cadresSheet = readFirstWorksheet(files.cadres.path)
    const prelevementsSheet = readFirstWorksheet(files.prelevements.path)

    if (!cadresSheet || !prelevementsSheet) {
      throw new Error(`[${CONNECTOR_NAME}] Un fichier GIDAF est vide.`)
    }

    const cadresRows = extractCadresRows(cadresSheet)
    const records = extractPrelevementRows(prelevementsSheet, cadresRows)

    console.log(
      `[${this.name}] Loaded records=${records.length}, cadres="${files.cadres.filename}", prelevements="${files.prelevements.filename}"`,
    )

    return {files, records}
  }

  protected async parse(
    rawData: GidafFetchResult,
    context: ConnectorRunContext,
  ): Promise<GidafParsedResult> {
    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: GidafConnector.connectorEnabledDate,
    })

    const records = rawData.records
      .filter((row) => rowMatchesSourcePointId(row, context.sourcePointId))
      .filter((row) => row.dateEnd.getTime() > startDate.getTime())

    console.log(
      `[${this.name}] Matched records=${records.length}, sourcePointId="${context.sourcePointId}", startDate=${startDate.toISOString()}`,
    )

    return {
      ...rawData,
      records,
    }
  }

  protected async process(
    parsedData: GidafParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const metrics = this.buildMetrics(parsedData.records)
    const {minDate, maxDate} = getDateBounds(parsedData.records)

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.DECLARATION,
      source_metadata: {
        provider: 'gidaf',
        cadres_file: parsedData.files.cadres.filename,
        prelevements_file: parsedData.files.prelevements.filename,
        row_count: parsedData.records.length,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics,
    }
  }

  private buildMetrics(records: GidafRawRow[]): ParsedPointPayload['metrics'] {
    const valuesByType = new Map<GidafMetricType, Map<number, TimeserieValue>>()

    for (const record of records) {
      const valuesByDate =
        valuesByType.get(record.metricType) ?? new Map<number, TimeserieValue>()
      const dateKey = record.dateEnd.getTime()
      const existing = valuesByDate.get(dateKey)

      valuesByDate.set(dateKey, {
        date: record.dateEnd,
        value: (existing?.value ?? 0) + record.value,
      })

      valuesByType.set(record.metricType, valuesByDate)
    }

    return [...valuesByType.entries()].map(([type, valuesByDate]) => ({
      type,
      granularity: Granularity.MONTH,
      conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
      values: sortValuesByDate([...valuesByDate.values()]),
      unit: MetricUnit.M3,
    }))
  }
}
