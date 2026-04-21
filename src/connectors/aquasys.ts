import {BaseConnector} from './base-connector.js'
import {
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'
import fs from 'node:fs/promises'
import path from 'node:path'
import * as XLSX from 'xlsx'

type AquasysRawRow = {
  sourceFile: string
  sourcePointId: string
  metricType: MetricType
  date: string
  value: number
}

type AquasysRowInput = {
  'Point de prélèvement': string
  'Index ou volume': string
  'Date de mesure': string
  Mesure: string | number
}

const AQUASYS_POINT_COLUMN = 'Point de prélèvement'
const AQUASYS_METRIC_COLUMN = 'Index ou volume'
const AQUASYS_DATE_COLUMN = 'Date de mesure'
const AQUASYS_VALUE_COLUMN = 'Mesure'

export class AquasysConnector extends BaseConnector {
  constructor() {
    super('aquasys')
  }

  protected async fetchSourceData(
    context: ConnectorRunContext,
  ): Promise<unknown> {
    const files = this.getSourceFiles(context)
    const rowsByFile = await Promise.all(
      files.map(async (file) => this.readRowsFromWorkbook(file)),
    )

    const records = rowsByFile
      .flat()
      .filter((row) => row.sourcePointId === context.sourcePointId)

    return {
      files,
      records,
    }
  }

  protected async parse(
    rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    if (!this.isAquasysFetchResult(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Aquasys data format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    const {metrics, minDate, maxDate} = this.buildMetrics(rawData.records)

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.BATCH,
      source_metadata: {
        provider: 'aquasys',
        files: rawData.files,
        row_count: rawData.records.length,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics,
    }
  }

  private getSourceFiles(context: ConnectorRunContext): string[] {
    if (context.sourceFiles && context.sourceFiles.length > 0) {
      return context.sourceFiles
    }

    return ['data/Dossiers_Consommations_30092024-31122025.xlsx']
  }

  private async readRowsFromWorkbook(filePath: string): Promise<AquasysRawRow[]> {
    const absolutePath = path.resolve(filePath)
    const buffer = await fs.readFile(absolutePath)
    const workbook = XLSX.read(buffer, {type: 'buffer'})
    const sheetName = workbook.SheetNames[0]
    if (!sheetName) {
      return []
    }

    const sheet = workbook.Sheets[sheetName]
    if (!sheet) {
      return []
    }

    const rows = XLSX.utils.sheet_to_json<AquasysRowInput>(sheet, {
      defval: '',
      raw: false,
    })

    return rows
      .map((row) => this.mapWorkbookRow(row, filePath))
      .filter((row): row is AquasysRawRow => row !== undefined)
  }

  private mapWorkbookRow(
    row: AquasysRowInput,
    sourceFile: string,
  ): AquasysRawRow | undefined {
    const sourcePointId = String(row[AQUASYS_POINT_COLUMN]).trim()
    const metricType = this.parseMetricType(row[AQUASYS_METRIC_COLUMN])
    const date = this.parseDate(row[AQUASYS_DATE_COLUMN])
    const value = this.parseNumber(row[AQUASYS_VALUE_COLUMN])

    if (!sourcePointId || !metricType || !date || value === undefined) {
      return undefined
    }

    return {
      sourceFile,
      sourcePointId,
      metricType,
      date,
      value,
    }
  }

  private parseMetricType(rawMetric: string): MetricType | undefined {
    const normalized = String(rawMetric).trim().toLowerCase()
    if (normalized === 'index') {
      return MetricType.INDEX
    }

    if (normalized === 'volume') {
      return MetricType.VOLUME_PRELEVE
    }

    return undefined
  }

  private parseDate(rawDate: string): string | undefined {
    const dateText = String(rawDate).trim()
    const match = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(dateText)
    if (!match) {
      return undefined
    }

    const [, day, month, year] = match
    return `${year}-${month}-${day}T00:00:00.000Z`
  }

  private parseNumber(rawValue: string | number): number | undefined {
    if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
      return rawValue
    }

    const cleaned = String(rawValue).replaceAll(/\s+/g, '').replace(',', '.')
    const parsed = Number(cleaned)
    if (!Number.isFinite(parsed)) {
      return undefined
    }

    return parsed
  }

  private isAquasysFetchResult(value: unknown): value is {
    files: string[]
    records: AquasysRawRow[]
  } {
    if (typeof value !== 'object' || value === null) {
      return false
    }

    const candidate = value as {files?: unknown; records?: unknown}
    return (
      Array.isArray(candidate.files) &&
      candidate.files.every((item) => typeof item === 'string') &&
      Array.isArray(candidate.records)
    )
  }

  private buildMetrics(records: AquasysRawRow[]): {
    metrics: ParsedPointPayload['metrics']
    minDate: string | undefined
    maxDate: string | undefined
  } {
    if (records.length === 0) {
      return {
        metrics: [],
        minDate: undefined,
        maxDate: undefined,
      }
    }

    const byType = new Map<MetricType, Array<{date: string; value: number}>>()
    for (const record of records) {
      const values = byType.get(record.metricType) ?? []
      values.push({date: record.date, value: record.value})
      byType.set(record.metricType, values)
    }

    const metrics = Array.from(byType.entries()).map(([type, values]) => ({
      type,
      granularity: Granularity.MONTH,
      values: values.sort((a, b) => a.date.localeCompare(b.date)),
      unit: MetricUnit.M3,
    }))

    const dates = records.map((record) => record.date)
    return {
      metrics,
      minDate: dates.reduce((currentMin, date) =>
        date < currentMin ? date : currentMin,
      ),
      maxDate: dates.reduce((currentMax, date) =>
        date > currentMax ? date : currentMax,
      ),
    }
  }
}
