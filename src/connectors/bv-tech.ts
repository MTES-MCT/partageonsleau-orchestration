import xlsx from 'xlsx'
import {BaseConnector} from './base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
  type Timeserie,
  type TimeserieValue,
} from './types.js'

const CONNECTOR_NAME = 'bv_tech'
const POINT_IDENTIFIER_ROW_INDEXES = [0, 1] as const
const HEADER_ROW_INDEX = 3
const DEBIT_UNIT_ROW_INDEX = 5
const DATA_START_ROW_INDEX = 6
const TIMESTAMP_COLUMN_INDEX = 0
const FIRST_MEASURE_COLUMN_INDEX = 1
const DEFAULT_DEBIT_INTERVAL_MINUTES = 5
const MAX_AUTOMATIC_DEBIT_INTERVAL_MINUTES = 60
const MILLISECONDS_PER_HOUR = 60 * 60 * 1000

type BvTechPointColumns = {
  sheetName: string
  pointIdentifiers: string[]
  debitColumn: number | undefined
  debitHeader: string | undefined
  debitUnit: string | undefined
  dailyVolumeColumn: number | undefined
  dailyVolumeHeader: string | undefined
}

type BvTechWorksheetCandidate = {
  sheetName: string
  rows: unknown[][]
  pointColumns: BvTechPointColumns[]
}

type BvTechParsedPoint = {
  rows: unknown[][]
  pointColumns: BvTechPointColumns
}

type DebitSample = {
  date: Date
  value: number
}

type ParsedExcelDateCode = {
  year: number
  month: number
  day: number
  hours: number
  minutes: number
  secondsWithFraction: number
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function getCellText(value: unknown): string | undefined {
  if (value === undefined || value === null) {
    return undefined
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    return trimmed.length > 0 ? trimmed : undefined
  }

  if (isFiniteNumber(value)) {
    return String(value)
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString()
  }

  return undefined
}

function normalizeText(value: string): string {
  return value
    .normalize('NFD')
    .replaceAll(/[\u0300-\u036F]/gv, '')
    .replaceAll(/\s+/gv, ' ')
    .trim()
    .toLowerCase()
}

function normalizeIdentifier(value: string): string {
  const normalized = normalizeText(value).replaceAll(/[^a-z0-9]/gv, '')

  if (/^\d+$/v.test(normalized)) {
    return normalized.replace(/^0+/v, '') || '0'
  }

  return normalized
}

function uniqueByIdentifier(values: string[]): string[] {
  const normalizedIdentifiers = new Set<string>()
  const uniqueValues: string[] = []

  for (const value of values) {
    const normalizedIdentifier = normalizeIdentifier(value)

    if (!normalizedIdentifiers.has(normalizedIdentifier)) {
      normalizedIdentifiers.add(normalizedIdentifier)
      uniqueValues.push(value)
    }
  }

  return uniqueValues
}

function pointKey(identifiers: string[]): string {
  return identifiers
    .map((identifier) => normalizeIdentifier(identifier))
    .toSorted()
    .join('|')
}

function parseNumericCell(value: unknown): number | undefined {
  if (isFiniteNumber(value)) {
    return value
  }

  if (typeof value !== 'string') {
    return undefined
  }

  const parsed = Number(
    value.trim().replaceAll(/\s+/gv, '').replaceAll(',', '.'),
  )

  return Number.isFinite(parsed) ? parsed : undefined
}

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

  const year = value.y
  const month = value.m
  const day = value.d
  const hours = value.H
  const minutes = value.M
  const secondsWithFraction = value.S

  if (
    !isFiniteNumber(year) ||
    !isFiniteNumber(month) ||
    !isFiniteNumber(day) ||
    !isFiniteNumber(hours) ||
    !isFiniteNumber(minutes) ||
    !isFiniteNumber(secondsWithFraction)
  ) {
    return undefined
  }

  return {
    year,
    month,
    day,
    hours,
    minutes,
    secondsWithFraction,
  }
}

function parseExcelDateCode(value: number): ParsedExcelDateCode | undefined {
  if (!isRecord(xlsx.SSF)) {
    return undefined
  }

  const parseDateCode = xlsx.SSF.parse_date_code

  if (!isParseDateCodeFunction(parseDateCode)) {
    return undefined
  }

  return toParsedExcelDateCode(parseDateCode(value))
}

function parseDateCell(value: unknown): Date | undefined {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value
  }

  if (isFiniteNumber(value)) {
    const parsed = parseExcelDateCode(value)

    if (!parsed) {
      return undefined
    }

    const seconds = Math.floor(parsed.secondsWithFraction)
    const milliseconds = Math.round(
      (parsed.secondsWithFraction - seconds) * 1000,
    )

    return new Date(
      Date.UTC(
        parsed.year,
        parsed.month - 1,
        parsed.day,
        parsed.hours,
        parsed.minutes,
        seconds,
        milliseconds,
      ),
    )
  }

  if (typeof value !== 'string') {
    return undefined
  }

  const trimmed = value.trim()
  const frenchDateMatch =
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/v.exec(
      trimmed,
    )

  if (frenchDateMatch) {
    const [, rawDay, rawMonth, rawYear, rawHour, rawMinute, rawSecond] =
      frenchDateMatch

    if (!rawDay || !rawMonth || !rawYear) {
      return undefined
    }

    const day = Number(rawDay)
    const month = Number(rawMonth)
    const year = Number(rawYear)
    const hour = rawHour ? Number(rawHour) : 0
    const minute = rawMinute ? Number(rawMinute) : 0
    const second = rawSecond ? Number(rawSecond) : 0

    return new Date(Date.UTC(year, month - 1, day, hour, minute, second))
  }

  const parsed = new Date(trimmed)
  return Number.isNaN(parsed.getTime()) ? undefined : parsed
}

function startOfUtcDay(date: Date): Date {
  return new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  )
}

function previousUtcDay(date: Date): Date {
  const previousDay = new Date(date)
  previousDay.setUTCDate(previousDay.getUTCDate() - 1)
  return previousDay
}

function sortValuesByDate(values: TimeserieValue[]): TimeserieValue[] {
  return values.toSorted(
    (left, right) => left.date.getTime() - right.date.getTime(),
  )
}

function getMaxColumnCount(rows: unknown[][]): number {
  let maxColumnCount = 0

  for (const row of rows.slice(0, DATA_START_ROW_INDEX)) {
    maxColumnCount = Math.max(maxColumnCount, row.length)
  }

  return maxColumnCount
}

function getCellTextWithLeftInheritance(
  rows: unknown[][],
  rowIndex: number,
  columnIndex: number,
): string | undefined {
  const directValue = getCellText(rows[rowIndex]?.[columnIndex])

  if (directValue) {
    return directValue
  }

  for (
    let inheritedColumnIndex = columnIndex - 1;
    inheritedColumnIndex >= FIRST_MEASURE_COLUMN_INDEX;
    inheritedColumnIndex -= 1
  ) {
    const inheritedValue = getCellText(rows[rowIndex]?.[inheritedColumnIndex])

    if (inheritedValue) {
      return inheritedValue
    }
  }

  return undefined
}

function getPointIdentifiersForColumn(
  rows: unknown[][],
  columnIndex: number,
): string[] {
  return uniqueByIdentifier(
    POINT_IDENTIFIER_ROW_INDEXES.flatMap((rowIndex) => {
      const identifier = getCellTextWithLeftInheritance(
        rows,
        rowIndex,
        columnIndex,
      )
      return identifier ? [identifier] : []
    }),
  )
}

function isDebitHeader(value: string): boolean {
  return normalizeText(value).includes('debit')
}

function isDailyVolumeHeader(value: string): boolean {
  const normalizedHeader = normalizeText(value)
  return (
    normalizedHeader.includes('volume') && !normalizedHeader.includes('debit')
  )
}

function getColumnName(columnIndex: number): string {
  let columnNumber = columnIndex + 1
  let columnName = ''

  while (columnNumber > 0) {
    const modulo = (columnNumber - 1) % 26
    columnName = String.fromCodePoint(65 + modulo) + columnName
    columnNumber = Math.floor((columnNumber - modulo) / 26)
  }

  return columnName
}

function findPointColumnsInRows(
  sheetName: string,
  rows: unknown[][],
): BvTechPointColumns[] {
  const pointColumnsByKey = new Map<string, BvTechPointColumns>()
  const maxColumnCount = getMaxColumnCount(rows)

  for (
    let columnIndex = FIRST_MEASURE_COLUMN_INDEX;
    columnIndex < maxColumnCount;
    columnIndex += 1
  ) {
    const header = getCellText(rows[HEADER_ROW_INDEX]?.[columnIndex])

    if (!header) {
      continue
    }

    const pointIdentifiers = getPointIdentifiersForColumn(rows, columnIndex)

    if (pointIdentifiers.length === 0) {
      continue
    }

    const key = pointKey(pointIdentifiers)
    const existingPointColumns = pointColumnsByKey.get(key)
    const pointColumns = existingPointColumns ?? {
      sheetName,
      pointIdentifiers,
      debitColumn: undefined,
      debitHeader: undefined,
      debitUnit: undefined,
      dailyVolumeColumn: undefined,
      dailyVolumeHeader: undefined,
    }

    if (isDebitHeader(header)) {
      pointColumns.debitColumn = columnIndex
      pointColumns.debitHeader = header
      pointColumns.debitUnit = getCellText(
        rows[DEBIT_UNIT_ROW_INDEX]?.[columnIndex],
      )
    }

    if (isDailyVolumeHeader(header)) {
      pointColumns.dailyVolumeColumn = columnIndex
      pointColumns.dailyVolumeHeader = header
    }

    pointColumnsByKey.set(key, pointColumns)
  }

  return [...pointColumnsByKey.values()].filter(
    (pointColumns) =>
      pointColumns.debitColumn !== undefined ||
      pointColumns.dailyVolumeColumn !== undefined,
  )
}

function readWorksheetCandidates(
  sourceFile: string,
): BvTechWorksheetCandidate[] {
  const workbook = xlsx.readFile(sourceFile, {cellDates: true})
  const candidates: BvTechWorksheetCandidate[] = []

  for (const sheetName of workbook.SheetNames) {
    const worksheet = workbook.Sheets[sheetName]

    if (!worksheet) {
      continue
    }

    const rows = xlsx.utils.sheet_to_json<unknown[]>(worksheet, {
      header: 1,
      raw: true,
      blankrows: false,
    })
    const pointColumns = findPointColumnsInRows(sheetName, rows)

    if (pointColumns.length > 0) {
      candidates.push({
        sheetName,
        rows,
        pointColumns,
      })
    }
  }

  return candidates
}

function pointColumnsMatchSourcePointId(
  pointColumns: BvTechPointColumns,
  sourcePointId: string,
): boolean {
  const normalizedSourcePointId = normalizeIdentifier(sourcePointId)
  return pointColumns.pointIdentifiers.some(
    (identifier) => normalizeIdentifier(identifier) === normalizedSourcePointId,
  )
}

function selectWorksheetForPoint(parameters: {
  candidates: BvTechWorksheetCandidate[]
  sourcePointId: string
}): BvTechParsedPoint {
  const {candidates, sourcePointId} = parameters
  const matchingPointColumns = candidates.flatMap((candidate) =>
    candidate.pointColumns
      .filter((pointColumns) =>
        pointColumnsMatchSourcePointId(pointColumns, sourcePointId),
      )
      .map((pointColumns) => ({
        rows: candidate.rows,
        pointColumns,
      })),
  )
  const matchingPointColumnsCandidate = matchingPointColumns[0]

  if (matchingPointColumns.length === 1 && matchingPointColumnsCandidate) {
    return matchingPointColumnsCandidate
  }

  if (matchingPointColumns.length > 1) {
    throw new Error(
      `[bv-tech] Plusieurs points du fichier correspondent à ${sourcePointId}.`,
    )
  }

  const onlyCandidate = candidates[0]
  const onlyPointColumns = onlyCandidate?.pointColumns[0]

  if (
    candidates.length === 1 &&
    onlyCandidate?.pointColumns.length === 1 &&
    onlyPointColumns
  ) {
    return {
      rows: onlyCandidate.rows,
      pointColumns: onlyPointColumns,
    }
  }

  const knownIdentifiers = candidates.flatMap((candidate) =>
    candidate.pointColumns.map((pointColumns) =>
      pointColumns.pointIdentifiers.join(' / '),
    ),
  )

  throw new Error(
    `[bv-tech] Aucun point du fichier ne correspond à ${sourcePointId}. ` +
      `Identifiants trouvés: ${knownIdentifiers.join(', ') || 'aucun'}.`,
  )
}

function buildDeclaredDailyVolumeValues(
  rows: unknown[][],
  pointColumns: BvTechPointColumns,
): TimeserieValue[] {
  const {dailyVolumeColumn} = pointColumns

  if (dailyVolumeColumn === undefined) {
    return []
  }

  const valuesByDay = new Map<number, TimeserieValue>()

  for (const row of rows.slice(DATA_START_ROW_INDEX)) {
    const timestamp = parseDateCell(row[TIMESTAMP_COLUMN_INDEX])
    const value = parseNumericCell(row[dailyVolumeColumn])

    if (!timestamp || value === undefined) {
      continue
    }

    // Dans le fichier BV Tech fourni, le volume journalier horodaté à J 00:00
    // correspond à la journée J-1.
    const date = previousUtcDay(startOfUtcDay(timestamp))
    valuesByDay.set(date.getTime(), {date, value})
  }

  return sortValuesByDate([...valuesByDay.values()])
}

function getDebitSamples(
  rows: unknown[][],
  pointColumns: BvTechPointColumns,
): DebitSample[] {
  const {debitColumn} = pointColumns

  if (debitColumn === undefined) {
    return []
  }

  return rows
    .slice(DATA_START_ROW_INDEX)
    .flatMap((row): DebitSample[] => {
      const date = parseDateCell(row[TIMESTAMP_COLUMN_INDEX])
      const value = parseNumericCell(row[debitColumn])

      if (!date || value === undefined) {
        return []
      }

      return [{date, value}]
    })
    .toSorted((left, right) => left.date.getTime() - right.date.getTime())
}

function isHourlyDebitUnit(unit: string | undefined): boolean {
  if (!unit) {
    return false
  }

  const normalizedUnit = normalizeText(unit).replaceAll(/\s+/gv, '')
  return (
    normalizedUnit.includes('/h') ||
    normalizedUnit.includes('h-1') ||
    normalizedUnit.includes('parheure')
  )
}

function getMedianDebitIntervalMilliseconds(samples: DebitSample[]): number {
  const maxIntervalMilliseconds =
    MAX_AUTOMATIC_DEBIT_INTERVAL_MINUTES * 60 * 1000
  const differences: number[] = []

  for (let index = 1; index < samples.length; index += 1) {
    const sample = samples[index]
    const previousSample = samples[index - 1]

    if (!sample || !previousSample) {
      continue
    }

    const difference = sample.date.getTime() - previousSample.date.getTime()

    if (difference > 0 && difference <= maxIntervalMilliseconds) {
      differences.push(difference)
    }
  }

  if (differences.length === 0) {
    return DEFAULT_DEBIT_INTERVAL_MINUTES * 60 * 1000
  }

  differences.sort((left, right) => left - right)
  return (
    differences[Math.floor(differences.length / 2)] ??
    DEFAULT_DEBIT_INTERVAL_MINUTES * 60 * 1000
  )
}

function buildDailyVolumeValuesFromDebit(
  rows: unknown[][],
  pointColumns: BvTechPointColumns,
): TimeserieValue[] {
  if (pointColumns.debitColumn === undefined) {
    return []
  }

  if (!isHourlyDebitUnit(pointColumns.debitUnit)) {
    throw new Error(
      `[bv-tech] L'unité du débit doit être horaire en ligne 6. Valeur trouvée: ${pointColumns.debitUnit ?? 'vide'}.`,
    )
  }

  const samples = getDebitSamples(rows, pointColumns)

  if (samples.length === 0) {
    return []
  }

  const fallbackIntervalMilliseconds =
    getMedianDebitIntervalMilliseconds(samples)
  const maxIntervalMilliseconds =
    MAX_AUTOMATIC_DEBIT_INTERVAL_MINUTES * 60 * 1000
  const valuesByDay = new Map<number, number>()

  for (const [index, sample] of samples.entries()) {
    const nextSample = samples[index + 1]
    const nextIntervalMilliseconds = nextSample
      ? nextSample.date.getTime() - sample.date.getTime()
      : undefined
    const intervalMilliseconds =
      nextIntervalMilliseconds &&
      nextIntervalMilliseconds > 0 &&
      nextIntervalMilliseconds <= maxIntervalMilliseconds
        ? nextIntervalMilliseconds
        : fallbackIntervalMilliseconds
    const day = startOfUtcDay(sample.date)
    const volume = sample.value * (intervalMilliseconds / MILLISECONDS_PER_HOUR)
    const dayKey = day.getTime()
    valuesByDay.set(dayKey, (valuesByDay.get(dayKey) ?? 0) + volume)
  }

  return sortValuesByDate(
    [...valuesByDay.entries()].map(([dayKey, value]) => ({
      date: new Date(dayKey),
      value,
    })),
  )
}

function filterValuesAfterMostRecentAvailableDate(
  values: TimeserieValue[],
  mostRecentAvailableDate: Date | undefined,
): TimeserieValue[] {
  if (!mostRecentAvailableDate) {
    return values
  }

  return values.filter(
    (value) => value.date.getTime() > mostRecentAvailableDate.getTime(),
  )
}

function buildParsedPayload(parameters: {
  context: ConnectorRunContext
  pointColumns: BvTechPointColumns
  rows: unknown[][]
}): ParsedPointPayload {
  const {context, pointColumns, rows} = parameters
  const declaredDailyVolumeValues = buildDeclaredDailyVolumeValues(
    rows,
    pointColumns,
  )
  const volumeValues =
    declaredDailyVolumeValues.length > 0
      ? declaredDailyVolumeValues
      : buildDailyVolumeValuesFromDebit(rows, pointColumns)
  const values = filterValuesAfterMostRecentAvailableDate(
    volumeValues,
    context.mostRecentAvailableDate,
  )
  const firstValue = values[0]
  const lastValue = values.at(-1)
  const metric: Timeserie = {
    type: MetricType.VOLUME_PRELEVE,
    granularity: Granularity.DAY,
    conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
    values,
    unit: MetricUnit.M3,
  }

  const payload: ParsedPointPayload = {
    id_point_de_prelevement: context.sourcePointId,
    source_type: SourceType.DECLARATION,
    source_metadata: {
      parser: 'bv-tech',
      sheetName: pointColumns.sheetName,
      pointIdentifiers: pointColumns.pointIdentifiers,
      volumeSource:
        declaredDailyVolumeValues.length > 0
          ? 'daily_volume_column'
          : 'debit_column',
      columns: {
        timestamp: getColumnName(TIMESTAMP_COLUMN_INDEX),
        debit:
          pointColumns.debitColumn === undefined
            ? undefined
            : getColumnName(pointColumns.debitColumn),
        dailyVolume:
          pointColumns.dailyVolumeColumn === undefined
            ? undefined
            : getColumnName(pointColumns.dailyVolumeColumn),
      },
      debitHeader: pointColumns.debitHeader,
      debitUnit: pointColumns.debitUnit,
      dailyVolumeHeader: pointColumns.dailyVolumeHeader,
      dailyVolumeTimestampPolicy:
        'La valeur horodatée à J 00:00 est rattachée à la journée J-1.',
    },
    metrics: [metric],
  } satisfies ParsedPointPayload

  if (firstValue) {
    payload.min_date = firstValue.date
  }

  if (lastValue) {
    payload.max_date = lastValue.date
  }

  return payload
}

export class BvTechConnector extends BaseConnector<
  BvTechWorksheetCandidate[],
  BvTechParsedPoint
> {
  constructor() {
    super(CONNECTOR_NAME)
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<BvTechWorksheetCandidate[]> {
    if (!context.sourceFile) {
      throw new Error(`[${CONNECTOR_NAME}] Un fichier source est obligatoire.`)
    }

    return readWorksheetCandidates(context.sourceFile)
  }

  protected async parse(
    candidates: BvTechWorksheetCandidate[],
    context: ConnectorRunContext,
  ): Promise<BvTechParsedPoint> {
    return selectWorksheetForPoint({
      candidates,
      sourcePointId: context.sourcePointId,
    })
  }

  protected async process(
    parsedPoint: BvTechParsedPoint,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    return buildParsedPayload({
      context,
      pointColumns: parsedPoint.pointColumns,
      rows: parsedPoint.rows,
    })
  }
}
