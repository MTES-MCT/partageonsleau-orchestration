import fs from 'node:fs/promises'
import path from 'node:path'
import moment from 'moment'
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

type TemplateFileRowInput = Record<string, unknown> & {
  id_point_de_prelevement?: string | number
  id_point_de_prelevement_ou_rejet?: string | number
  date_debut?: string | number | Date
  date_fin?: string | number | Date
  volume_preleve_m3?: string | number
  volume_rejete_m3?: string | number
  usage?: string
  Usage?: string
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

const ID_COLUMNS = [
  'id_point_de_prelevement',
  'id_point_de_prelevement_ou_rejet',
] as const

const DATE_START_COLUMN = 'date_debut'
const DATE_END_COLUMN = 'date_fin'
const VOLUME_COLUMN = 'volume_preleve_m3'

const DECLARATION_DATE_FORMATS = [
  'DD/MM/YYYY',
  'D/M/YYYY',
  'DD-MM-YYYY',
  'D-M-YYYY',
  'YYYY-MM-DD',
  'YYYY/MM/DD',
  'DD/MM/YYYY HH:mm:ss',
  'DD/MM/YYYY HH:mm',
  'YYYY-MM-DD HH:mm:ss',
  'YYYY-MM-DD HH:mm',
] as const

function normalizePointIdentifier(value: string): string {
  return value
    .trim()
    .normalize('NFC')
    .toLocaleLowerCase('fr-FR')
    .replaceAll(/\s+/g, ' ')
}

function getSourcePointId(row: TemplateFileRowInput): string | undefined {
  for (const column of ID_COLUMNS) {
    const value = row[column]
    const text = String(value ?? '').trim()

    if (text) {
      return text
    }
  }

  return undefined
}

function parseExcelSerialDate(rawDate: number): Date | undefined {
  if (!Number.isFinite(rawDate)) {
    return undefined
  }

  const parsedDate = XLSX.SSF.parse_date_code(rawDate)

  if (!parsedDate) {
    return undefined
  }

  const date = moment.utc({
    year: parsedDate.y,
    month: parsedDate.m - 1,
    date: parsedDate.d,
    hour: parsedDate.H,
    minute: parsedDate.M,
    second: Math.floor(parsedDate.S),
    millisecond: 0,
  })

  return date.isValid() ? date.toDate() : undefined
}

function parseDeclarationDate(rawDate: unknown): Date | undefined {
  if (rawDate instanceof Date) {
    const date = moment.utc(rawDate)

    return date.isValid() ? date.toDate() : undefined
  }

  if (typeof rawDate === 'number') {
    return parseExcelSerialDate(rawDate)
  }

  const text = String(rawDate ?? '').trim()

  if (!text) {
    return undefined
  }

  const parsedDate = moment.utc(text, DECLARATION_DATE_FORMATS, true)

  if (parsedDate.isValid()) {
    return parsedDate.toDate()
  }

  const parsedIsoDate = moment.utc(text, moment.ISO_8601, true)

  return parsedIsoDate.isValid() ? parsedIsoDate.toDate() : undefined
}

function parseDeclarationNumber(rawValue: unknown): number | undefined {
  if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
    return rawValue
  }

  const cleaned = String(rawValue ?? '')
    .trim()
    .replaceAll(/\s/g, '')
    .replace(',', '.')

  if (!cleaned) {
    return undefined
  }

  const parsed = Number(cleaned)

  return Number.isFinite(parsed) ? parsed : undefined
}

function parseTemplateVolumeRow(
  row: TemplateFileRowInput,
): TemplateFileRawRow | undefined {
  const sourcePointId = getSourcePointId(row)
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

async function readRowsFromWorkbook(
  filePath: string,
  sheetName: string,
): Promise<TemplateFileRowInput[]> {
  const absolutePath = path.resolve(filePath)
  const buffer = await fs.readFile(absolutePath)

  const workbook = XLSX.read(buffer, {
    type: 'buffer',
    cellDates: true,
  })

  const sheet = workbook.Sheets[sheetName]

  if (!sheet) {
    console.warn(
      `[template_file] Sheet "${sheetName}" not found in file "${absolutePath}". Available sheets: ${workbook.SheetNames.join(', ')}`,
    )
    return []
  }

  return XLSX.utils.sheet_to_json<TemplateFileRowInput>(sheet, {
    defval: '',
    raw: true,
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

    const rows = await readRowsFromWorkbook(filePath, TEMPLATE_SHEET_NAME)

    console.log(
      `[${this.name}] Loaded rows=${rows.length}, file="${filePath}", sheet="${TEMPLATE_SHEET_NAME}"`,
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

    const normalizedSourcePointId = normalizePointIdentifier(
      context.sourcePointId,
    )

    const parsedRows = rawData.rows
      .map((row) => parseTemplateVolumeRow(row))
      .filter((row): row is TemplateFileRawRow => row !== undefined)

    const availableSourcePointIds = [
      ...new Set(parsedRows.map((row) => row.sourcePointId)),
    ]

    console.log(
      `[${this.name}] Parsed rows=${parsedRows.length}, sourcePointId="${context.sourcePointId}", startDate=${startDate.toISOString()}`,
    )

    console.log(
      `[${this.name}] Available source IDs sample:`,
      availableSourcePointIds.slice(0, 10),
    )

    const matchingRows = parsedRows.filter(
      (row) =>
        normalizePointIdentifier(row.sourcePointId) === normalizedSourcePointId,
    )

    console.log(
      `[${this.name}] Matching rows before date filter=${matchingRows.length}, sourcePointId="${context.sourcePointId}"`,
    )

    const records = matchingRows.filter(
      (row) => row.dateStart.getTime() > startDate.getTime(),
    )

    console.log(
      `[${this.name}] Matched records=${records.length}, sourcePointId="${context.sourcePointId}"`,
    )

    return {records}
  }

  protected async process(
    parsedData: TemplateFileParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const byType = new Map<MetricType, Array<{date: Date; value: number}>>()

    for (const record of parsedData.records) {
      const values = byType.get(record.metricType) ?? []

      values.push({
        date: record.dateStart,
        value: record.value,
      })

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
