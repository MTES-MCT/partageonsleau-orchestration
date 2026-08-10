import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  PointFlowType,
  SourceType,
  type ConnectorDiscoveryContext,
  type ConnectorRunContext,
  type ParsedPointPayload,
  type TimeserieValue,
  type WaterUseCode,
} from './types.js'
import {BaseConnector} from './base-connector.js'
import {
  normalizePointIdentifier,
  parseDeclarationDate,
  parseDeclarationNumber,
  readSpreadsheetSheet,
} from './spreadsheet.js'

type TemplateFileRowInput = Record<string, unknown> & {
  id_point_de_prelevement?: string | number
  id_point_de_prelevement_ou_rejet?: string | number
  date_debut?: string | number | Date
  date_fin?: string | number | Date
  volume_m3?: string | number
  volume_preleve_m3?: string | number
  volume_rejete_m3?: string | number
  usage?: string | number
}

type TemplateFileRawRow = {
  sourcePointId: string
  flowType?: PointFlowType
  metricType: MetricType.VOLUME
  usage?: WaterUseCode
  dateStart: Date
  dateEnd: Date
  periodEnd: Date
  granularity: Granularity
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
const VOLUME_COLUMNS = [
  {name: 'volume_m3', flowType: undefined},
  {name: 'volume_preleve_m3', flowType: PointFlowType.PRELEVEMENT},
  {name: 'volume_rejete_m3', flowType: PointFlowType.REJET},
] as const
const USAGE_COLUMN = 'usage'
const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000

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

/**
 * Les colonnes date_debut/date_fin du template portent un jour civil, jamais
 * un horaire. Selon le fuseau du processus, xlsx peut toutefois convertir une
 * cellule Excel en un instant proche de la veille au soir (par exemple 22:59
 * en hiver ou 21:59 en été). On neutralise d'abord le fuseau appliqué par xlsx
 * aux cellules Date, puis on arrondit au minuit UTC le plus proche. Les chaînes
 * et numéros déjà interprétés en UTC ne sont pas décalés. Le parseur partagé,
 * qui sert aussi à des connecteurs réellement horodatés, reste inchangé.
 */
export function normalizeTemplateDateOnly(rawDate: unknown): Date | undefined {
  const parsedDate = parseDeclarationDate(rawDate)

  if (!parsedDate) {
    return undefined
  }

  const timezoneNeutralTimestamp =
    rawDate instanceof Date
      ? parsedDate.getTime() - parsedDate.getTimezoneOffset() * 60 * 1000
      : parsedDate.getTime()

  return new Date(
    Math.round(timezoneNeutralTimestamp / MILLISECONDS_PER_DAY) *
      MILLISECONDS_PER_DAY,
  )
}

export function getExclusiveTemplatePeriodEnd(dateEnd: Date): Date {
  return new Date(dateEnd.getTime() + MILLISECONDS_PER_DAY)
}

function getUtcMonthIndex(date: Date): number {
  return date.getUTCFullYear() * 12 + date.getUTCMonth()
}

export function inferTemplateGranularity(
  dateStart: Date,
  inclusiveDateEnd: Date,
): Granularity {
  const periodEnd = getExclusiveTemplatePeriodEnd(inclusiveDateEnd)
  const durationDays =
    (periodEnd.getTime() - dateStart.getTime()) / MILLISECONDS_PER_DAY
  const startsOnFirstDay = dateStart.getUTCDate() === 1
  const endsBeforeFirstDay = periodEnd.getUTCDate() === 1
  const monthCount = getUtcMonthIndex(periodEnd) - getUtcMonthIndex(dateStart)

  if (
    startsOnFirstDay &&
    endsBeforeFirstDay &&
    dateStart.getUTCMonth() === 0 &&
    monthCount === 12
  ) {
    return Granularity.YEAR
  }

  if (
    startsOnFirstDay &&
    endsBeforeFirstDay &&
    dateStart.getUTCMonth() % 3 === 0 &&
    monthCount === 3
  ) {
    return Granularity.QUARTER
  }

  if (startsOnFirstDay && endsBeforeFirstDay && monthCount === 1) {
    return Granularity.MONTH
  }

  if (durationDays === 7) {
    return Granularity.WEEK
  }

  return Granularity.DAY
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
  const dateStart = normalizeTemplateDateOnly(row[DATE_START_COLUMN])
  const dateEnd = normalizeTemplateDateOnly(row[DATE_END_COLUMN])
  const usage = parseTemplateUsageCode(row[USAGE_COLUMN])
  const populatedVolumeColumns = VOLUME_COLUMNS.flatMap((column) => {
    const value = parseDeclarationNumber(row[column.name])

    return value === undefined ? [] : [{...column, value}]
  })

  if (populatedVolumeColumns.length > 1) {
    throw new Error(
      `[template_file] Plusieurs colonnes de volume sont renseignées pour le point "${sourcePointId ?? 'inconnu'}". Utilisez uniquement volume_m3.`,
    )
  }

  const volume = populatedVolumeColumns[0]

  if (
    !sourcePointId ||
    !dateStart ||
    !dateEnd ||
    dateEnd.getTime() < dateStart.getTime() ||
    !volume
  ) {
    return undefined
  }

  return {
    sourcePointId,
    flowType: volume.flowType,
    metricType: MetricType.VOLUME,
    usage,
    dateStart,
    dateEnd,
    periodEnd: getExclusiveTemplatePeriodEnd(dateEnd),
    granularity: inferTemplateGranularity(dateStart, dateEnd),
    value: volume.value,
  }
}

export class TemplateFileConnector extends BaseConnector<
  TemplateFileFetchResult,
  TemplateFileParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')
  private static readonly metric = {
    type: MetricType.VOLUME,
    conflictPolicy: ConflictPolicy.REPLACE_EXISTING_EXCEPT_WILLIE,
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

    const {rows} = await readSpreadsheetSheet<TemplateFileRowInput>(
      context.sourceFile,
      TEMPLATE_SHEET_NAME,
      {connectorName: this.name},
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

    const {rows} = await readSpreadsheetSheet<TemplateFileRowInput>(
      filePath,
      TEMPLATE_SHEET_NAME,
      {connectorName: this.name},
    )

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

    const explicitFlowTypes = new Set(
      matchingRows.flatMap((row) => (row.flowType ? [row.flowType] : [])),
    )

    if (explicitFlowTypes.size > 1) {
      throw new Error(
        `[${this.name}] Le fichier mélange prélèvement et rejet pour le point "${context.sourcePointId}".`,
      )
    }

    const explicitFlowType = [...explicitFlowTypes][0]
    if (
      explicitFlowType &&
      context.flowType &&
      explicitFlowType !== context.flowType
    ) {
      throw new Error(
        `[${this.name}] La colonne de volume indique ${explicitFlowType}, mais le point "${context.sourcePointId}" est configuré en ${context.flowType}.`,
      )
    }

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
        granularity: Granularity
        values: TimeserieValue[]
      }
    >()

    for (const record of parsedData.records) {
      const key = `${record.metricType}__${record.usage ?? 'NO_USAGE'}__${record.granularity}`
      const group = byTypeAndUsage.get(key) ?? {
        type: record.metricType,
        usage: record.usage,
        granularity: record.granularity,
        values: [],
      }

      group.values.push({
        date: record.dateEnd,
        periodStart: record.dateStart,
        periodEnd: record.periodEnd,
        value: record.value,
      })

      byTypeAndUsage.set(key, group)
    }

    const metrics = [...byTypeAndUsage.values()].map(
      ({type, usage, granularity, values}) => ({
        type,
        ...(usage ? {usage} : {}),
        granularity,
        conflictPolicy: TemplateFileConnector.metric.conflictPolicy,
        values,
        unit: TemplateFileConnector.metric.unit,
      }),
    )

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
      flow_type:
        parsedData.records.find((record) => record.flowType)?.flowType ??
        context.flowType,
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
