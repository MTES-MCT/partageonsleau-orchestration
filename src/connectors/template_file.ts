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

type TemplateFileRowInput = {
  id_point_de_prelevement: string
  date_debut: string | number | Date
  date_fin: string | number | Date
  volume_preleve_m3: string | number
  volume_rejete_m3: string | number
  Usage: string
}

type TemplateFileRawRow = {
  sourcePointId: string
  metricType: MetricType.VOLUME_PRELEVE
  dateStart: Date
  dateEnd: Date | undefined
  value: number
}

type TemplateFileFetchResult = {
  rows: TemplateFileRowInput[]
}

type TemplateFileParsedResult = {
  records: TemplateFileRawRow[]
}

const TEMPLATE_SHEET_NAME = 'declaration_de_volume'
const ID_COLUMN = 'id_point_de_prelevement'
const DATE_START_COLUMN = 'date_debut'
const DATE_END_COLUMN = 'date_fin'
const VOLUME_COLUMN = 'volume_preleve_m3'

function parseFrenchDate(rawDate: string): Date | undefined {
  const dateText = rawDate.trim()
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

function parseDeclarationDate(
  rawDate: string | number | Date,
): Date | undefined {
  if (rawDate instanceof Date) {
    return Number.isNaN(rawDate.getTime()) ? undefined : rawDate
  }

  const text = String(rawDate).trim()
  if (!text) {
    return undefined
  }

  return (
    parseFrenchDate(text) ??
    (() => {
      const parsed = new Date(text)
      return Number.isNaN(parsed.getTime()) ? undefined : parsed
    })()
  )
}

function parseDeclarationNumber(rawValue: string | number): number | undefined {
  if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
    return rawValue
  }

  const cleaned = String(rawValue).replaceAll(' ', '').replace(',', '.')
  const parsed = Number(cleaned)
  return Number.isFinite(parsed) ? parsed : undefined
}

function parseTemplateVolumeRow(
  row: TemplateFileRowInput,
): TemplateFileRawRow | undefined {
  const sourcePointId = String(row[ID_COLUMN]).trim()
  const dateStart = parseDeclarationDate(row[DATE_START_COLUMN])
  const dateEnd = parseDeclarationDate(row[DATE_END_COLUMN])
  const volumeValue = parseDeclarationNumber(row[VOLUME_COLUMN])

  if (!sourcePointId || !dateStart || volumeValue === undefined) {
    return undefined
  }

  return {
    sourcePointId,
    metricType: MetricType.VOLUME_PRELEVE,
    dateStart,
    dateEnd,
    value: volumeValue,
  }
}

async function readRowsFromWorkbook<TInput extends Record<string, unknown>>(
  filePath: string,
  sheetName: string,
): Promise<TInput[]> {
  const absolutePath = path.resolve(filePath)
  const buffer = await fs.readFile(absolutePath)
  const workbook = XLSX.read(buffer, {type: 'buffer'})
  const sheet = workbook.Sheets[sheetName]
  if (!sheet) {
    return []
  }

  return XLSX.utils.sheet_to_json<TInput>(sheet, {
    defval: '',
    raw: false,
  })
}

export class TemplateFileConnector extends BaseConnector<
  TemplateFileFetchResult,
  TemplateFileParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')
  private static readonly metric = {
    type: MetricType.VOLUME_PRELEVE,
    granularity: Granularity.DAY,
    unit: MetricUnit.M3,
  } as const

  constructor() {
    super('template_file')
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<TemplateFileFetchResult> {
    const filePath =
      context.sourceFile ?? 'data/declaration_valloire_gallaure_11_2025.xlsx'
    const rows = await readRowsFromWorkbook<TemplateFileRowInput>(
      filePath,
      TEMPLATE_SHEET_NAME,
    )

    return {rows}
  }

  protected async parse(
    rawData: TemplateFileFetchResult,
    context: ConnectorRunContext,
  ): Promise<TemplateFileParsedResult> {
    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: TemplateFileConnector.connectorEnabledDate,
    })
    const records = rawData.rows
      .map((row) => parseTemplateVolumeRow(row))
      .filter((row): row is TemplateFileRawRow => row !== undefined)
      .filter((row) => row.sourcePointId === context.sourcePointId)
      .filter((row) => row.dateStart.getTime() > startDate.getTime())

    return {records}
  }

  protected async process(
    parsedData: TemplateFileParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const byType = new Map<MetricType, Array<{date: Date; value: number}>>()
    for (const record of parsedData.records) {
      const values = byType.get(record.metricType) ?? []
      values.push({date: record.dateStart, value: record.value})
      byType.set(record.metricType, values)
    }

    const metrics = [...byType.entries()].map(([type, values]) => ({
      type,
      granularity: TemplateFileConnector.metric.granularity,
      values: values.map((value) => ({
        date: value.date,
        value: value.value,
      })),
      unit: TemplateFileConnector.metric.unit,
    }))

    const {minDate, maxDate} = this.getMinMaxDates(
      parsedData.records,
      (record) => record.dateStart,
    )

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.BATCH,
      source_metadata: {
        provider: 'template_file',
        sheet_name: TEMPLATE_SHEET_NAME,
        row_count: parsedData.records.length,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics,
    }
  }
}
