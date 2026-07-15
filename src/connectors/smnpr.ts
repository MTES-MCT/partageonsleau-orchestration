import {BaseConnector} from './base-connector.js'
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
} from './types.js'
import {
  getSpreadsheetCellText,
  normalizePointIdentifier,
  parseDeclarationDate,
  parseDeclarationNumber,
  readSpreadsheetSheet,
  type SpreadsheetRow,
} from './spreadsheet.js'

const CONNECTOR_NAME = 'smnpr'
const SHEET_NAME = 'declaration_de_volume'
const SOURCE_POINT_COLUMN = 'id_point_de_prelevement_ou_rejet'
const DATE_START_COLUMN = 'date_debut'
const DATE_END_COLUMN = 'date_fin'
const VOLUME_COLUMN = 'volume_preleve_m3'
const EXPECTED_HEADERS = [
  SOURCE_POINT_COLUMN,
  DATE_START_COLUMN,
  DATE_END_COLUMN,
  VOLUME_COLUMN,
] as const
const BSS_ID_PATTERN = /^BSS[A-Z\d]+$/v
const SOURCE_POINT_SEPARATOR_PATTERN = /[,;\n]+/v

type SmnprRowInput = SpreadsheetRow & {
  id_point_de_prelevement_ou_rejet?: string | number
  date_debut?: string | number | Date
  date_fin?: string | number | Date
  volume_preleve_m3?: string | number
}

type SmnprRecord = {
  sourcePointId: string
  dateStart: Date
  dateEnd: Date
  value: number
}

type SmnprFetchResult = {
  rows: SmnprRowInput[]
}

type SmnprParsedResult = {
  records: SmnprRecord[]
}

function validateHeaders(headers: string[]): void {
  if (
    headers.length !== EXPECTED_HEADERS.length ||
    headers.some((header, index) => header !== EXPECTED_HEADERS[index])
  ) {
    throw new Error(
      `[${CONNECTOR_NAME}] En-têtes invalides dans la feuille "${SHEET_NAME}". ` +
        `Colonnes attendues : ${EXPECTED_HEADERS.join(', ')}.`,
    )
  }
}

function parseSourcePointIds(value: unknown): string[] {
  const sourcePointIds = [
    ...new Set(
      (getSpreadsheetCellText(value) ?? '')
        .split(SOURCE_POINT_SEPARATOR_PATTERN)
        .map((sourcePointId) => sourcePointId.trim().toLocaleUpperCase('fr-FR'))
        .filter(Boolean),
    ),
  ]

  if (
    sourcePointIds.length === 0 ||
    sourcePointIds.some((sourcePointId) => !BSS_ID_PATTERN.test(sourcePointId))
  ) {
    return []
  }

  return sourcePointIds
}

function parseRows(rows: SmnprRowInput[]): SmnprRecord[] {
  if (rows.length === 0) {
    throw new Error(`[${CONNECTOR_NAME}] Le fichier ne contient aucune donnée.`)
  }

  const records: SmnprRecord[] = []
  const seenPeriods = new Set<string>()

  for (const [index, row] of rows.entries()) {
    const rowNumber = index + 2
    const sourcePointIds = parseSourcePointIds(row[SOURCE_POINT_COLUMN])
    const dateStart = parseDeclarationDate(row[DATE_START_COLUMN])
    const dateEnd = parseDeclarationDate(row[DATE_END_COLUMN])
    const value = parseDeclarationNumber(row[VOLUME_COLUMN])

    if (sourcePointIds.length === 0) {
      throw new Error(
        `[${CONNECTOR_NAME}] Ligne ${rowNumber} : code BSS invalide.`,
      )
    }

    if (!dateStart) {
      throw new Error(
        `[${CONNECTOR_NAME}] Ligne ${rowNumber} : date de début invalide.`,
      )
    }

    if (!dateEnd || dateEnd.getTime() <= dateStart.getTime()) {
      throw new Error(
        `[${CONNECTOR_NAME}] Ligne ${rowNumber} : la date de fin doit être postérieure à la date de début.`,
      )
    }

    if (value === undefined || value < 0) {
      throw new Error(
        `[${CONNECTOR_NAME}] Ligne ${rowNumber} : le volume prélevé doit être un nombre positif ou nul.`,
      )
    }

    const distributedValue = value / sourcePointIds.length

    for (const sourcePointId of sourcePointIds) {
      const periodKey = [
        normalizePointIdentifier(sourcePointId),
        dateStart.toISOString(),
        dateEnd.toISOString(),
      ].join('__')

      if (seenPeriods.has(periodKey)) {
        throw new Error(
          `[${CONNECTOR_NAME}] Ligne ${rowNumber} : cette période est déjà déclarée pour le point ${sourcePointId}.`,
        )
      }

      seenPeriods.add(periodKey)
      records.push({
        sourcePointId,
        dateStart,
        dateEnd,
        value: distributedValue,
      })
    }
  }

  return records
}

async function readSmnprFile(filePath: string): Promise<{
  rows: SmnprRowInput[]
  records: SmnprRecord[]
}> {
  const {headers, rows} = await readSpreadsheetSheet<SmnprRowInput>(
    filePath,
    SHEET_NAME,
    {
      connectorName: CONNECTOR_NAME,
      required: true,
    },
  )

  validateHeaders(headers)

  return {rows, records: parseRows(rows)}
}

export class SmnprConnector extends BaseConnector<
  SmnprFetchResult,
  SmnprParsedResult
> {
  private static readonly connectorEnabledDate = new Date('2026-01-01')

  constructor() {
    super(CONNECTOR_NAME)
  }

  async discoverSourcePointIds(
    context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    if (!context.sourceFile) {
      return []
    }

    const {records} = await readSmnprFile(context.sourceFile)

    return [...new Set(records.map((record) => record.sourcePointId))]
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<SmnprFetchResult> {
    if (!context.sourceFile) {
      throw new Error(`[${this.name}] Aucun fichier source fourni.`)
    }

    const {rows} = await readSmnprFile(context.sourceFile)

    console.log(
      `[${this.name}] Loaded rows=${rows.length}, file="${context.sourceFile}", sheet="${SHEET_NAME}"`,
    )

    return {rows}
  }

  protected async parse(
    rawData: SmnprFetchResult,
    context: ConnectorRunContext,
  ): Promise<SmnprParsedResult> {
    if (context.flowType && context.flowType !== PointFlowType.PRELEVEMENT) {
      throw new Error(
        `[${this.name}] Le point "${context.sourcePointId}" est configuré en rejet alors que le fichier contient des prélèvements.`,
      )
    }

    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: SmnprConnector.connectorEnabledDate,
    })
    const normalizedSourcePointId = normalizePointIdentifier(
      context.sourcePointId,
    )
    const records = parseRows(rawData.rows).filter(
      (record) =>
        normalizePointIdentifier(record.sourcePointId) ===
          normalizedSourcePointId &&
        record.dateStart.getTime() > startDate.getTime(),
    )

    console.log(
      `[${this.name}] Matched records=${records.length}, sourcePointId="${context.sourcePointId}"`,
    )

    return {records}
  }

  protected async process(
    parsedData: SmnprParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const values = parsedData.records.map((record) => ({
      date: record.dateEnd,
      periodStart: record.dateStart,
      periodEnd: record.dateEnd,
      value: record.value,
    }))
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
      flow_type: PointFlowType.PRELEVEMENT,
      source_type: SourceType.BATCH,
      source_metadata: {
        provider: CONNECTOR_NAME,
        sheet_name: SHEET_NAME,
        bss_id: context.sourcePointId,
        row_count: parsedData.records.length,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: MetricType.VOLUME,
          granularity: Granularity.DAY,
          conflictPolicy: ConflictPolicy.REPLACE_EXISTING,
          values,
          unit: MetricUnit.M3,
        },
      ],
    }
  }
}
