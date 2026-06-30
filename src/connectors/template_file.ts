import fs from 'node:fs/promises'
import path from 'node:path'
import moment from 'moment'
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
  type WaterUseCode,
} from './types.js'
import {BaseConnector} from './base-connector.js'

type TemplateFileRowInput = Record<string, unknown> & {
  id_point_de_prelevement?: string | number
  id_point_de_prelevement_ou_rejet?: string | number
  date_debut?: string | number | Date
  date_fin?: string | number | Date
  volume_preleve_m3?: string | number
  volume_rejete_m3?: string | number
  usage?: string | number
}

type TemplateFileRawRow = {
  sourcePointId: string
  metricType: MetricType.VOLUME_PRELEVE
  usage?: WaterUseCode
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
const USAGE_COLUMN = 'usage'

const SANDRE_WATER_USE_CODES = [
  '0',
  '1',
  '2',
  '2A',
  '2B',
  '2C',
  '2D',
  '2E',
  '2F',
  '3',
  '3A',
  '3B',
  '4',
  '4A',
  '4B',
  '4C',
  '4D',
  '5',
  '5A',
  '5B',
  '6',
  '6A',
  '6B',
  '6C',
  '6C1',
  '6C2',
  '6C3',
  '6D',
  '7',
  '7A',
  '7B',
  '7C',
  '7D',
  '7E',
  '8',
  '9',
  '9A',
  '9B',
  '10',
  '11',
  '12',
  '12A',
  '12B',
  '12C',
  '12D',
  '12E',
  '13',
  '13A',
  '13B',
  '14',
  '15',
  '16',
  '17',
] as const

const SANDRE_WATER_USE_CODE_SET: ReadonlySet<string> = new Set(
  SANDRE_WATER_USE_CODES,
)

const LEGACY_USAGE_TO_SANDRE_CODE: Readonly<Record<string, WaterUseCode>> = {
  INCONNU: '0',
  PAS_D_USAGE: '1',
  IRRIGATION: '2',
  AGRICULTURE_ELEVAGE: '3',
  AQUACULTURE: '3B',
  INDUSTRIE: '4',
  AEP: '5',
  ENERGIE: '6',
  LOISIRS: '7',
  EMBOUTEILLAGE: '8',
  THERMALISME_THALASSO: '9',
  DEFENSE_INCENDIE: '10',
  REALIMENTATION_EAU: '12',
  CANAUX: '13',
  ETIAGE: '14',
  ENTRETIEN_VOIRIES: '15',
  ALIMENTATION_SOUTIEN_CANAL: '16',
  DOMESTIQUE: '17',
}

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
]

function normalizePointIdentifier(value: string): string {
  return value
    .trim()
    .normalize('NFC')
    .toLocaleLowerCase('fr-FR')
    .replaceAll(/\s+/gv, ' ')
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

  const excelEpochUtc = Date.UTC(1899, 11, 30)
  const millisecondsInDay = 24 * 60 * 60 * 1000
  const timestamp = excelEpochUtc + Math.round(rawDate * millisecondsInDay)
  const date = new Date(timestamp)

  if (Number.isNaN(date.getTime())) {
    return undefined
  }

  return date
}

function parseDeclarationDate(rawDate: unknown): Date | undefined {
  if (rawDate instanceof Date) {
    const date = moment.utc(rawDate)

    return date.isValid() ? date.toDate() : undefined
  }

  if (typeof rawDate === 'number') {
    return parseExcelSerialDate(rawDate)
  }

  if (typeof rawDate !== 'string') {
    return undefined
  }

  const text = rawDate.trim()

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

  if (typeof rawValue !== 'string') {
    return undefined
  }

  const cleaned = rawValue.trim().replaceAll(/\s/gv, '').replace(',', '.')

  if (!cleaned) {
    return undefined
  }

  const parsed = Number(cleaned)

  return Number.isFinite(parsed) ? parsed : undefined
}

function parseTemplateUsageCode(rawUsage: unknown): WaterUseCode | undefined {
  if (typeof rawUsage !== 'string' && typeof rawUsage !== 'number') {
    return undefined
  }

  const usage = String(rawUsage).trim().toLocaleUpperCase('fr-FR')

  if (!usage) {
    return undefined
  }

  if (SANDRE_WATER_USE_CODE_SET.has(usage)) {
    return usage
  }

  return LEGACY_USAGE_TO_SANDRE_CODE[usage]
}

function parseTemplateVolumeRow(
  row: TemplateFileRowInput,
): TemplateFileRawRow | undefined {
  const sourcePointId = getSourcePointId(row)
  const dateStart = parseDeclarationDate(row[DATE_START_COLUMN])
  const dateEnd = parseDeclarationDate(row[DATE_END_COLUMN])
  const volumeValue = parseDeclarationNumber(row[VOLUME_COLUMN])
  const usage = parseTemplateUsageCode(row[USAGE_COLUMN])

  if (!sourcePointId || !dateStart || volumeValue === undefined) {
    return undefined
  }

  return {
    sourcePointId,
    metricType: MetricType.VOLUME_PRELEVE,
    usage,
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
    conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
    unit: MetricUnit.M3,
  } as const

  constructor() {
    super('template_file')
  }

  async discoverSourcePointIds(
    context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    if (!context.sourceFile) {
      return []
    }

    const rows = await readRowsFromWorkbook(
      context.sourceFile,
      TEMPLATE_SHEET_NAME,
    )
    const sourcePointIds = rows.flatMap((row) => {
      const sourcePointId = getSourcePointId(row)

      return sourcePointId ? [sourcePointId] : []
    })

    return [...new Set(sourcePointIds)]
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
    const byTypeAndUsage = new Map<
      string,
      {
        type: MetricType
        usage: WaterUseCode | undefined
        values: Array<{date: Date; value: number}>
      }
    >()

    for (const record of parsedData.records) {
      const key = `${record.metricType}__${record.usage ?? 'NO_USAGE'}`
      const group = byTypeAndUsage.get(key) ?? {
        type: record.metricType,
        usage: record.usage,
        values: [],
      }

      group.values.push({
        date: record.dateStart,
        value: record.value,
      })

      byTypeAndUsage.set(key, group)
    }

    const metrics = [...byTypeAndUsage.values()].map(
      ({type, usage, values}) => ({
        type,
        ...(usage ? {usage} : {}),
        granularity: TemplateFileConnector.metric.granularity,
        conflictPolicy: TemplateFileConnector.metric.conflictPolicy,
        values: values.map((value) => ({
          date: value.date,
          value: value.value,
        })),
        unit: TemplateFileConnector.metric.unit,
      }),
    )

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
