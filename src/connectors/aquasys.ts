import fs from 'node:fs/promises'
import path from 'node:path'
import * as XLSX from 'xlsx'
import {
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
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
  metricType: MetricType.INDEX
}

type AquasysVolumeRow = AquasysBaseRow & {
  metricType: MetricType.VOLUME_PRELEVE
  dateEnd: Date
}

type AquasysRawRow = AquasysIndexRow | AquasysVolumeRow

type AquasysFetchResult = {
  rows: AquasysRowInput[]
}

type AquasysParsedResult = {
  records: AquasysRawRow[]
}

type AquasysRowInput = {
  'Point de prélèvement': string
  'Index ou volume': string
  'Date de mesure': string
  'Date de fin': string | undefined
  Mesure: string | number
}

const AQUASYS_POINT_COLUMN = 'Point de prélèvement'
const AQUASYS_METRIC_COLUMN = 'Index ou volume'
const AQUASYS_DATE_COLUMN = 'Date de mesure'
const AQUASYS_DATE_END_COLUMN = 'Date de fin'
const AQUASYS_VALUE_COLUMN = 'Mesure'

function parseAquasysMetricType(rawMetric: string): MetricType | undefined {
  const normalized = String(rawMetric).trim().toLowerCase()
  if (normalized === 'index') {
    return MetricType.INDEX
  }

  if (normalized === 'volume') {
    return MetricType.VOLUME_PRELEVE
  }

  return undefined
}

function parseAquasysDate(rawDate: string | undefined): Date | undefined {
  if (!rawDate) {
    return undefined
  }

  const dateText = String(rawDate).trim()
  const parts = dateText.split('/')
  if (parts.length !== 3) {
    return undefined
  }

  const [day, month, year] = parts
  if (
    !day ||
    !month ||
    !year ||
    day.length !== 2 ||
    month.length !== 2 ||
    year.length !== 4
  ) {
    return undefined
  }

  const parsedDate = new Date(`${year}-${month}-${day}T00:00:00.000Z`)
  if (Number.isNaN(parsedDate.getTime())) {
    return undefined
  }

  return parsedDate
}

function parseAquasysNumber(rawValue: string | number): number | undefined {
  if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
    return rawValue
  }

  const cleaned = String(rawValue).replaceAll(' ', '').replace(',', '.')
  const parsed = Number(cleaned)
  if (!Number.isFinite(parsed)) {
    return undefined
  }

  return parsed
}

function parseAquasysWorkbookRow(
  row: AquasysRowInput,
): AquasysRawRow | undefined {
  const sourcePointId = String(row[AQUASYS_POINT_COLUMN]).trim()
  const metricType = parseAquasysMetricType(row[AQUASYS_METRIC_COLUMN])
  const dateStart = parseAquasysDate(row[AQUASYS_DATE_COLUMN])
  const dateEnd = parseAquasysDate(row[AQUASYS_DATE_END_COLUMN])
  const value = parseAquasysNumber(row[AQUASYS_VALUE_COLUMN])

  if (!sourcePointId || !metricType || !dateStart || value === undefined) {
    return undefined
  }

  if (metricType === MetricType.VOLUME_PRELEVE) {
    if (!dateEnd) {
      return undefined
    }

    return {
      sourcePointId,
      metricType,
      dateStart,
      dateEnd,
      value,
    }
  }

  return {
    sourcePointId,
    metricType,
    dateStart,
    value,
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
    raw: false,
  })

  return rows
}

export class AquasysConnector extends BaseConnector<
  AquasysFetchResult,
  AquasysParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')
  private static readonly metric = {
    granularity: Granularity.DAY,
    unit: MetricUnit.M3,
    supportedTypes: [MetricType.INDEX, MetricType.VOLUME_PRELEVE],
  } as const

  constructor() {
    super('aquasys')
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
    const records = rawData.rows
      .map((row) => parseAquasysWorkbookRow(row))
      .filter((row): row is AquasysRawRow => row !== undefined)
      .filter((row) => row.sourcePointId === context.sourcePointId)
      .filter(
        (row) =>
          row.dateStart.getTime() >
          this.resolveStartDate({
            mostRecentAvailableDate: context.mostRecentAvailableDate,
            connectorEnabledDate: AquasysConnector.connectorEnabledDate,
          }).getTime(),
      )

    return {records}
  }

  protected async process(
    parsedData: AquasysParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const metrics = this.buildMetrics(parsedData.records)

    const {minDate, maxDate} = this.getMinMaxDates(
      parsedData.records,
      (record) => record.dateStart,
    )
    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.BATCH,
      source_metadata: {
        provider: 'aquasys',
        row_count: parsedData.records.length,
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
    records: AquasysRawRow[],
  ): ParsedPointPayload['metrics'] {
    if (records.length === 0) {
      return []
    }

    // Split les records en fonction de leur type: index ou volume
    const byType = new Map<MetricType, Array<{date: Date; value: number}>>()
    for (const record of records) {
      const values = byType.get(record.metricType) ?? []
      values.push({date: record.dateStart, value: record.value})
      byType.set(record.metricType, values)
    }

    const metrics = [...byType.entries()].map(([type, values]) => ({
      type,
      granularity: AquasysConnector.metric.granularity,
      values: values.map((value) => ({
        date: value.date,
        value: value.value,
      })),
      unit: AquasysConnector.metric.unit,
    }))

    return metrics
  }
}
