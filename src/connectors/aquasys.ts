import fs from 'node:fs/promises'
import path from 'node:path'
import * as XLSX from 'xlsx'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorDiscoveryContext,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'
import {BaseConnector} from './base-connector.js'

type AquasysBaseRow = {
  sourcePointId: string
  dateStart: Date
  value: number
}

type AquasysIndexRow = AquasysBaseRow & {
  compteur: string
  coefficient: number
}

type AquasysVolumeRow = AquasysBaseRow & {
  dateEnd: Date
}

type AquasysRawRow = AquasysIndexRow | AquasysVolumeRow

type AquasysComputedVolumeRow = {
  sourcePointId: string
  dateStart: Date
  dateEnd: Date
  value: number
}

type AquasysPointVolumes = {
  records: AquasysComputedVolumeRow[]
  granularity: Granularity
}

type AquasysFetchResult = {
  rows: AquasysRowInput[]
}

type AquasysParsedResult = {
  records: AquasysComputedVolumeRow[]
  granularity: Granularity
  rawRowCount: number
}

type AquasysRowInput = {
  'Point de prélèvement': string
  'Index ou volume': string
  'Date de mesure': string | number | Date
  'Date de fin': string | number | Date | undefined
  Mesure: string | number
  'Coefficient de lecture': string | number | undefined
  Compteur: string | undefined
}

const AQUASYS_POINT_COLUMN = 'Point de prélèvement'
const AQUASYS_METRIC_COLUMN = 'Index ou volume'
const AQUASYS_DATE_COLUMN = 'Date de mesure'
const AQUASYS_DATE_END_COLUMN = 'Date de fin'
const AQUASYS_VALUE_COLUMN = 'Mesure'
const AQUASYS_READING_COEFFICIENT_COLUMN = 'Coefficient de lecture'
const AQUASYS_METER_COLUMN = 'Compteur'

function parseAquasysMetricType(rawMetric: string): MetricType {
  const normalized = String(rawMetric).trim().toLowerCase()
  if (normalized.startsWith('volume')) {
    return MetricType.VOLUME_PRELEVE
  }

  return MetricType.INDEX
}

function parseExcelDate(rawDate: number): Date | undefined {
  if (!Number.isFinite(rawDate)) {
    return undefined
  }

  const startDate = new Date(Date.UTC(1900, 0, 1))
  return new Date(startDate.getTime() + (rawDate - 2) * 86_400_000)
}

function parseAquasysDate(
  rawDate: string | number | Date | undefined,
): Date | undefined {
  if (!rawDate) {
    return undefined
  }

  if (typeof rawDate === 'number') {
    return parseExcelDate(rawDate)
  }

  if (rawDate instanceof Date) {
    return Number.isNaN(rawDate.getTime()) ? undefined : rawDate
  }

  const dateText = String(rawDate).trim()
  const isoDate = /^\d{4}-\d{2}-\d{2}$/v.exec(dateText)
  if (isoDate) {
    const parsedDate = new Date(`${dateText}T00:00:00.000Z`)
    return Number.isNaN(parsedDate.getTime()) ? undefined : parsedDate
  }

  const parts = dateText.split('/')
  if (parts.length !== 3) {
    return undefined
  }

  const [day, month, year] = parts
  if (!day || !month || year?.length !== 4) {
    return undefined
  }

  const paddedDay = day.padStart(2, '0')
  const paddedMonth = month.padStart(2, '0')
  const parsedDate = new Date(
    `${year}-${paddedMonth}-${paddedDay}T00:00:00.000Z`,
  )
  if (Number.isNaN(parsedDate.getTime())) {
    return undefined
  }

  return parsedDate
}

function parseAquasysNumber(rawValue: string | number): number | undefined {
  if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
    return rawValue
  }

  const cleaned = String(rawValue)
    .replaceAll(/[\s\u00A0\u202F]+/gv, '')
    .replace(',', '.')
  if (!cleaned) {
    return undefined
  }

  const parsed = Number(cleaned)
  if (!Number.isFinite(parsed)) {
    return undefined
  }

  return parsed
}

function toDateKey(date: Date): string {
  return date.toISOString().slice(0, 10)
}

function diffInDays(start: Date, end: Date): number {
  return Math.round((end.getTime() - start.getTime()) / 86_400_000)
}

function inferGranularity(durations: number[]): Granularity {
  if (durations.length === 0) {
    return Granularity.DAY
  }

  const sortedDurations = durations.toSorted((left, right) => left - right)
  const median = sortedDurations[Math.floor(sortedDurations.length / 2)]

  if (median >= 330) {
    return Granularity.YEAR
  }

  if (median >= 80) {
    return Granularity.QUARTER
  }

  if (median >= 25) {
    return Granularity.MONTH
  }

  return Granularity.DAY
}

function parseAquasysWorkbookRow(
  row: AquasysRowInput,
): AquasysRawRow | undefined {
  const sourcePointId = String(row[AQUASYS_POINT_COLUMN]).trim()
  const metricType = parseAquasysMetricType(row[AQUASYS_METRIC_COLUMN])
  const dateStart = parseAquasysDate(row[AQUASYS_DATE_COLUMN])
  const dateEnd = parseAquasysDate(row[AQUASYS_DATE_END_COLUMN]) ?? dateStart
  const value = parseAquasysNumber(row[AQUASYS_VALUE_COLUMN])
  const coefficient =
    parseAquasysNumber(row[AQUASYS_READING_COEFFICIENT_COLUMN] ?? 1) ?? 1
  const compteur = String(row[AQUASYS_METER_COLUMN] ?? 'default').trim()

  if (!sourcePointId || !dateStart || value === undefined) {
    return undefined
  }

  if (metricType === MetricType.VOLUME_PRELEVE) {
    if (!dateEnd) {
      return undefined
    }

    return {
      sourcePointId,
      dateStart,
      dateEnd,
      value,
    }
  }

  return {
    sourcePointId,
    dateStart,
    compteur: compteur || 'default',
    coefficient,
    value,
  }
}

function isAquasysVolumeRow(row: AquasysRawRow): row is AquasysVolumeRow {
  return 'dateEnd' in row
}

function computeVolumesFromIndex(
  indexRows: AquasysIndexRow[],
): AquasysComputedVolumeRow[] {
  const rowsByKey = new Map<string, AquasysIndexRow[]>()

  for (const row of indexRows) {
    const key = `${row.sourcePointId}__${row.compteur || 'default'}`
    const values = rowsByKey.get(key) ?? []
    values.push(row)
    rowsByKey.set(key, values)
  }

  const computedRows: AquasysComputedVolumeRow[] = []

  for (const rows of rowsByKey.values()) {
    const rowsByDate = new Map<string, AquasysIndexRow>()

    for (const row of rows) {
      const dateKey = toDateKey(row.dateStart)
      const existing = rowsByDate.get(dateKey)

      if (!existing || row.value > existing.value) {
        rowsByDate.set(dateKey, row)
      }
    }

    const uniqueRows = [...rowsByDate.values()].toSorted(
      (left, right) => left.dateStart.getTime() - right.dateStart.getTime(),
    )

    for (let index = 1; index < uniqueRows.length; index++) {
      const previous = uniqueRows[index - 1]
      const current = uniqueRows[index]
      const diff = current.value - previous.value
      const coefficient = Number.isFinite(current.coefficient)
        ? current.coefficient
        : 1
      const volume =
        diff >= 0 ? diff * coefficient : current.value * coefficient

      if (!Number.isFinite(volume)) {
        continue
      }

      computedRows.push({
        sourcePointId: current.sourcePointId,
        dateStart: previous.dateStart,
        dateEnd: current.dateStart,
        value: volume,
      })
    }
  }

  return computedRows
}

function consolidatePointVolumes(
  volumeRows: AquasysComputedVolumeRow[],
): AquasysPointVolumes {
  const valuesByDate = new Map<number, AquasysComputedVolumeRow>()
  const durations: number[] = []

  for (const row of volumeRows) {
    const duration = diffInDays(row.dateStart, row.dateEnd)
    if (Number.isFinite(duration) && duration >= 0) {
      durations.push(duration)
    }

    const dateKey = row.dateEnd.getTime()
    const existing = valuesByDate.get(dateKey)
    valuesByDate.set(dateKey, {
      sourcePointId: row.sourcePointId,
      dateStart: existing?.dateStart ?? row.dateStart,
      dateEnd: row.dateEnd,
      value: (existing?.value ?? 0) + row.value,
    })
  }

  return {
    records: [...valuesByDate.values()].toSorted(
      (left, right) => left.dateEnd.getTime() - right.dateEnd.getTime(),
    ),
    granularity: inferGranularity(durations),
  }
}

async function readRowsFromWorkbook<TInput extends Record<string, unknown>>(
  filePath: string,
  sheetName?: string,
): Promise<TInput[]> {
  const absolutePath = path.resolve(filePath)
  const buffer = await fs.readFile(absolutePath)
  const workbook = XLSX.read(buffer, {type: 'buffer'})

  const resolvedSheetName = sheetName ?? workbook.SheetNames[0]
  if (!resolvedSheetName) {
    return []
  }

  const sheet = workbook.Sheets[resolvedSheetName]
  if (!sheet) {
    return []
  }

  const rows = XLSX.utils.sheet_to_json<TInput>(sheet, {
    defval: '',
    raw: true,
  })

  return rows
}

export class AquasysConnector extends BaseConnector<
  AquasysFetchResult,
  AquasysParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')
  private static readonly metric = {
    unit: MetricUnit.M3,
    conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
  } as const

  constructor() {
    super('aquasys')
  }

  async discoverSourcePointIds(
    context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    if (!context.sourceFile) {
      return []
    }

    const rows = await readRowsFromWorkbook<AquasysRowInput>(
      context.sourceFile,
      'Export',
    )
    const sourcePointIds = rows.flatMap((row) => {
      const sourcePointId = parseAquasysWorkbookRow(row)?.sourcePointId

      return sourcePointId ? [sourcePointId] : []
    })

    return [...new Set(sourcePointIds)]
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<AquasysFetchResult> {
    const file = this.getSourceFile(context)
    const rows = await readRowsFromWorkbook<AquasysRowInput>(file, 'Export')

    return {rows}
  }

  protected async parse(
    rawData: AquasysFetchResult,
    context: ConnectorRunContext,
  ): Promise<AquasysParsedResult> {
    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: AquasysConnector.connectorEnabledDate,
    })

    const rawRows = rawData.rows
      .map((row) => parseAquasysWorkbookRow(row))
      .filter((row): row is AquasysRawRow => row !== undefined)
      .filter((row) => row.sourcePointId === context.sourcePointId)

    const indexRows = rawRows.filter(
      (row): row is AquasysIndexRow => !isAquasysVolumeRow(row),
    )
    const explicitVolumeRows = rawRows.filter((row): row is AquasysVolumeRow =>
      isAquasysVolumeRow(row),
    )
    const volumeRows = [
      ...computeVolumesFromIndex(indexRows),
      ...explicitVolumeRows,
    ].filter((row) => row.dateEnd.getTime() > startDate.getTime())
    const {records, granularity} = consolidatePointVolumes(volumeRows)

    return {
      records,
      granularity,
      rawRowCount: rawRows.length,
    }
  }

  protected async process(
    parsedData: AquasysParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const metrics = this.buildMetrics(parsedData)

    const {minDate} = this.getMinMaxDates(
      parsedData.records,
      (record) => record.dateStart,
    )
    const {maxDate} = this.getMinMaxDates(
      parsedData.records,
      (record) => record.dateEnd,
    )
    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.BATCH,
      source_metadata: {
        provider: 'aquasys',
        row_count: parsedData.rawRowCount,
        volume_row_count: parsedData.records.length,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics,
    }
  }

  private getSourceFile(context: ConnectorRunContext): string {
    if (context.sourceFile) {
      return context.sourceFile
    }

    return 'data/Dossiers_Consommations_30092024-31122025.xlsx'
  }

  private buildMetrics(
    parsedData: AquasysParsedResult,
  ): ParsedPointPayload['metrics'] {
    if (parsedData.records.length === 0) {
      return []
    }

    return [
      {
        type: MetricType.VOLUME_PRELEVE,
        granularity: parsedData.granularity,
        conflictPolicy: AquasysConnector.metric.conflictPolicy,
        values: parsedData.records.map((record) => ({
          date: record.dateEnd,
          value: record.value,
        })),
        unit: AquasysConnector.metric.unit,
      },
    ]
  }
}
