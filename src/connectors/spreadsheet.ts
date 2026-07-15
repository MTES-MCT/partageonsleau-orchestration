import fs from 'node:fs/promises'
import path from 'node:path'
import moment from 'moment'
import * as XLSX from 'xlsx'

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

export type SpreadsheetRow = Record<string, unknown>

export type SpreadsheetSheet<TRow extends SpreadsheetRow> = {
  headers: string[]
  rows: TRow[]
}

export function normalizePointIdentifier(value: string): string {
  return value
    .trim()
    .normalize('NFC')
    .toLocaleLowerCase('fr-FR')
    .replaceAll(/\s+/gv, ' ')
}

export function getSpreadsheetCellText(value: unknown): string | undefined {
  if (typeof value === 'string') {
    const text = value.trim()
    return text.length > 0 ? text : undefined
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value)
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

  return Number.isNaN(date.getTime()) ? undefined : date
}

export function parseDeclarationDate(rawDate: unknown): Date | undefined {
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

export function parseDeclarationNumber(rawValue: unknown): number | undefined {
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

export async function readSpreadsheetSheet<TRow extends SpreadsheetRow>(
  filePath: string,
  sheetName: string,
  options: {
    connectorName: string
    required?: boolean
  },
): Promise<SpreadsheetSheet<TRow>> {
  const absolutePath = path.resolve(filePath)
  const buffer = await fs.readFile(absolutePath)
  const workbook = XLSX.read(buffer, {
    type: 'buffer',
    cellDates: true,
  })
  const sheet = workbook.Sheets[sheetName]

  if (!sheet) {
    const message =
      `[${options.connectorName}] Sheet "${sheetName}" not found in file ` +
      `"${absolutePath}". Available sheets: ${workbook.SheetNames.join(', ')}`

    if (options.required) {
      throw new Error(message)
    }

    console.warn(message)
    return {headers: [], rows: []}
  }

  const [rawHeaders = []] = XLSX.utils.sheet_to_json<unknown[]>(sheet, {
    header: 1,
    defval: '',
    raw: true,
    blankrows: false,
  })
  const headers = rawHeaders.map(
    (header) => getSpreadsheetCellText(header) ?? '',
  )
  const rows = XLSX.utils.sheet_to_json<TRow>(sheet, {
    defval: '',
    raw: true,
  })

  return {headers, rows}
}
