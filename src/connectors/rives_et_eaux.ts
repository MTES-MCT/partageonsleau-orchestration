import {createHash} from 'node:crypto'
import type {MeterReading} from '../services/meter-readings-client.js'

export const RIVES_ET_EAUX_PROVIDER = 'rives-et-eaux'
export const RIVES_ET_EAUX_SCOPE = 'epidropt'
export const RIVES_ET_EAUX_TIMEZONE = 'Europe/Paris'
const admittedQualities = new Set(['A', 'B', 'C', 'D', 'E', 'X'])
const excludedQualities = new Set(['Y', 'W', 'Z'])
const dayMilliseconds = 86_400_000
const localFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: RIVES_ET_EAUX_TIMEZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
})

export type RivesDayWindow = {
  localStart: string
  localEnd: string
  windowStart: string
  windowEnd: string
}

export type RivesTemporalResult = {
  temporalStatus: 'VALID' | 'INVALID_LOCAL_TIME' | 'AMBIGUOUS_LOCAL_TIME'
  normalizedObservedAt?: string
}

function formatLocal(date: Date): string {
  const parts = Object.fromEntries(
    localFormatter.formatToParts(date).map((part) => [part.type, part.value]),
  )
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`
}

/** Preserve seconds and reject both sides of an ambiguous autumn local time. */
export function parseRivesLocalDate(value: unknown): RivesTemporalResult {
  if (typeof value !== 'string') {
    return {temporalStatus: 'INVALID_LOCAL_TIME'}
  }

  const match =
    /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d{1,3}))?$/v.exec(value)
  if (!match) {
    return {temporalStatus: 'INVALID_LOCAL_TIME'}
  }

  const local = `${match[1]}T${match[2]}`
  const fraction = (match[3] ?? '').padEnd(3, '0')
  const guess = new Date(`${local}.${fraction}Z`)
  if (
    Number.isNaN(guess.getTime()) ||
    guess.toISOString().slice(0, 19) !== local
  ) {
    return {temporalStatus: 'INVALID_LOCAL_TIME'}
  }

  const offsets = new Set<number>()
  for (const delta of [-36, 0, 36]) {
    const probe = new Date(guess.getTime() + delta * 3_600_000)
    offsets.add(
      new Date(`${formatLocal(probe)}.${fraction}Z`).getTime() -
        probe.getTime(),
    )
  }

  const candidates = [...offsets]
    .map((offset) => new Date(guess.getTime() - offset))
    .filter((candidate) => formatLocal(candidate) === local)

  if (candidates.length !== 1) {
    return {
      temporalStatus:
        candidates.length > 1 ? 'AMBIGUOUS_LOCAL_TIME' : 'INVALID_LOCAL_TIME',
    }
  }

  return {
    temporalStatus: 'VALID',
    normalizedObservedAt: candidates[0].toISOString(),
  }
}

export function rivesDayWindows(now = new Date()): RivesDayWindow[] {
  const today = formatLocal(now).slice(0, 10)
  const end = new Date(`${today}T00:00:00.000Z`).getTime()
  return Array.from({length: 15}, (_, index) => {
    const localStart = new Date(end - (15 - index) * dayMilliseconds)
      .toISOString()
      .slice(0, 10)
    const localEnd = new Date(end - (14 - index) * dayMilliseconds)
      .toISOString()
      .slice(0, 10)
    return {
      localStart,
      localEnd,
      windowStart: parseRivesLocalDate(`${localStart}T00:00:00`)
        .normalizedObservedAt!,
      windowEnd: parseRivesLocalDate(`${localEnd}T00:00:00`)
        .normalizedObservedAt!,
    }
  })
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(',')}]`
  }

  if (typeof value === 'object' && value !== null) {
    return `{${Object.entries(value)
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`)
      .join(',')}}`
  }

  return JSON.stringify(value) ?? 'null'
}

export function rivesFingerprint(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex')
}

function nullableText(value: unknown): string | null {
  return typeof value === 'string' &&
    value.trim().length > 0 &&
    value.trim().length <= 250
    ? value.trim()
    : null
}

/** Decimal(20,4), without rounding or binary arithmetic on cumulative indices. */
function normalizeRivesIndex(value: unknown): string | null {
  if (
    typeof value === 'number' &&
    (!Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER)
  ) {
    return null
  }

  if (typeof value !== 'number' && typeof value !== 'string') {
    return null
  }

  const match = /^(\d+)(?:\.(\d{1,4}))?$/v.exec(
    String(value).trim().replaceAll(',', '.'),
  )
  if (!match) {
    return null
  }

  const integer = match[1].replace(/^0+(?=\d)/v, '')
  if (integer.length > 16) {
    return null
  }

  const fraction = (match[2] ?? '').replace(/0+$/v, '')
  return fraction ? `${integer}.${fraction}` : integer
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function rivesExternalId(value: unknown): string | null {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) && value >= 0 ? String(value) : null
  }

  return nullableText(value)
}

function normalizeRivesRow(raw: unknown): MeterReading {
  if (!isRecord(raw)) {
    return {
      externalId: null,
      observedAt: null,
      index: null,
      status: 'INVALID',
      reason: 'INVALID_ROW',
      quality: null,
      origin: null,
      raw,
    }
  }

  const row = raw
  const externalId = rivesExternalId(row.NumeroSerieCompteur)
  const temporal = parseRivesLocalDate(row.Date)
  const index = normalizeRivesIndex(row.Index)
  const quality = nullableText(row.CodeValidite)?.toUpperCase() ?? null
  const reason = externalId
    ? temporal.temporalStatus === 'VALID'
      ? index === null
        ? 'INVALID_INDEX'
        : excludedQualities.has(quality ?? '')
          ? 'EXCLUDED_QUALITY'
          : admittedQualities.has(quality ?? '')
            ? null
            : 'INVALID_QUALITY'
      : temporal.temporalStatus
    : 'INVALID_EXTERNAL_ID'

  return {
    externalId,
    observedAt: temporal.normalizedObservedAt ?? null,
    index,
    status: reason ? 'INVALID' : 'VALID',
    reason,
    quality,
    origin: nullableText(row.Origine),
    raw,
  }
}

/** Deduplicate strict repeats, not conflicting readings or unlocated anomalies across windows. */
export function normalizeRivesRows(
  rows: unknown[],
  seen = new Set<string>(),
): MeterReading[] {
  const output: MeterReading[] = []
  const windowSeen = new Set<string>()
  for (const row of rows) {
    const fingerprint = rivesFingerprint(row)
    const normalized = normalizeRivesRow(row)
    if (
      windowSeen.has(fingerprint) ||
      (normalized.observedAt !== null && seen.has(fingerprint))
    ) {
      continue
    }

    windowSeen.add(fingerprint)
    if (normalized.observedAt !== null) {
      seen.add(fingerprint)
    }

    output.push(normalized)
  }

  return output
}

export class RivesEtEauxConnector {
  private readonly endpoint: URL

  constructor(
    private readonly apiKey: string,
    baseUrl = 'https://services.riveseteaux.fr',
    private readonly fetcher: typeof fetch = fetch,
  ) {
    const base = new URL(baseUrl)
    if (
      base.protocol !== 'https:' ||
      base.username ||
      base.password ||
      base.search ||
      base.hash ||
      base.pathname !== '/'
    ) {
      throw new Error(
        '[rives-et-eaux] RIVES_ET_EAUX_BASE_URL must be an HTTPS origin without credentials.',
      )
    }

    if (!apiKey) {
      throw new Error('[rives-et-eaux] Missing RIVES_ET_EAUX_API_KEY.')
    }

    this.endpoint = new URL('/api/public/Calypso/Export', base)
  }

  async fetchWindow(window: RivesDayWindow): Promise<unknown[]> {
    const url = new URL(this.endpoint)
    url.searchParams.set('dateDebut', window.localStart)
    url.searchParams.set('dateFin', window.localEnd)
    let response: Response
    try {
      response = await this.fetcher(url, {
        headers: {Accept: 'application/json', 'X-API-Key': this.apiKey},
        signal: AbortSignal.timeout(30_000),
        redirect: 'error',
      })
    } catch {
      throw new Error('[rives-et-eaux] Provider request failed or timed out.')
    }

    if (!response.ok) {
      throw new Error(`[rives-et-eaux] Provider HTTP ${response.status}.`)
    }

    let body: unknown
    try {
      body = await response.json()
    } catch {
      throw new Error('[rives-et-eaux] Provider response is not valid JSON.')
    }

    if (!Array.isArray(body)) {
      throw new TypeError(
        '[rives-et-eaux] Provider response must be a complete JSON array.',
      )
    }

    return body as unknown[]
  }
}
