import {createHash} from 'node:crypto'
import {mkdir, readFile, writeFile} from 'node:fs/promises'
import {join} from 'node:path'
import {setTimeout as delay} from 'node:timers/promises'
import {BaseConnector} from './base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
  type TimeserieValue,
} from './types.js'

const hour = 3_600_000
const step = 600_000
const day = 24 * hour
const origin = 'https://api.eveler.pro'
const maximumWindows = 30

function requestGate(
  wait: (milliseconds: number) => Promise<void>,
  clock: () => number,
) {
  let previous = Promise.resolve()
  let lastStartedAt = -Infinity
  return () => {
    const next = previous.then(async () => {
      const remaining = 1_100 - (clock() - lastStartedAt)
      if (remaining > 0) await wait(remaining)
      lastStartedAt = clock()
    })
    previous = next.catch(() => undefined)
    return next
  }
}

// Shared by normal clients in this process, including authentication and retries.
const providerRequestGate = requestGate(delay, () => performance.now())

export type EvelerWindow = {start: Date; end: Date}
type EvelerResponse = {
  success: true
  data: {
    meter_id: string
    start_date: string
    end_date: string
    unit: 'm3'
    timestep: 600
    values: unknown[]
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function timestamp(value: unknown): number {
  if (
    typeof value !== 'string' ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+\-]\d{2}:\d{2})$/v.test(
      value,
    )
  ) {
    return NaN
  }
  const local = new Date(`${value.slice(0, 19)}Z`)
  if (
    !Number.isFinite(local.getTime()) ||
    local.toISOString().slice(0, 19) !== value.slice(0, 19)
  )
    return NaN
  return Date.parse(value)
}

function validateWindow(window: EvelerWindow): void {
  if (
    !Number.isFinite(window.start.getTime()) ||
    !Number.isFinite(window.end.getTime()) ||
    window.start.getTime() % hour !== 0 ||
    window.end.getTime() % hour !== 0 ||
    window.end <= window.start ||
    window.end.getTime() - window.start.getTime() > maximumWindows * 364 * day
  ) {
    throw new Error('[eveler] Expected a bounded, positive UTC hourly window.')
  }
}

function validateResponse(
  value: unknown,
  sourceMeterId: string,
): EvelerResponse {
  if (
    !isRecord(value) ||
    value.success !== true ||
    !isRecord(value.data) ||
    value.data.meter_id !== sourceMeterId ||
    typeof value.data.start_date !== 'string' ||
    !Number.isFinite(Date.parse(value.data.start_date)) ||
    typeof value.data.end_date !== 'string' ||
    !Number.isFinite(Date.parse(value.data.end_date)) ||
    value.data.unit !== 'm3' ||
    value.data.timestep !== 600 ||
    !Array.isArray(value.data.values) ||
    value.data.values.length > 110_000
  ) {
    throw new Error(
      '[eveler] Invalid volume response (identity, unit, timestep or shape).',
    )
  }
  return value as EvelerResponse
}

/** Native fetch deliberately preserves environment proxy, TLS and DNS settings. */
export class EvelerClient {
  private readonly identifier: string | undefined
  private readonly secret: string | undefined
  private readonly fetcher: typeof fetch
  private readonly now: () => Date
  private readonly beforeRequest: () => Promise<void>
  private token: string | undefined
  private expiresAt = 0

  constructor(
    options: {
      identifier?: string
      secret?: string
      baseUrl?: string
      fetcher?: typeof fetch
      now?: () => Date
      wait?: (milliseconds: number) => Promise<void>
      requestClock?: () => number
    } = {},
  ) {
    this.identifier = options.identifier ?? process.env.EVELER_API_IDENTIFIER
    this.secret = options.secret ?? process.env.EVELER_API_SECRET
    this.fetcher = options.fetcher ?? fetch
    this.now = options.now ?? (() => new Date())
    this.beforeRequest =
      options.wait || options.requestClock
        ? requestGate(
            options.wait ?? delay,
            options.requestClock ?? (() => performance.now()),
          )
        : providerRequestGate
    const base = options.baseUrl ?? process.env.EVELER_API_BASE_URL ?? origin
    // Credentials travel in the login query: no configurable third-party host.
    if (base !== origin && base !== `${origin}/`) {
      throw new Error(
        '[eveler] EVELER_API_BASE_URL must be https://api.eveler.pro.',
      )
    }
  }

  static isConfigured(): boolean {
    return Boolean(
      process.env.EVELER_API_IDENTIFIER && process.env.EVELER_API_SECRET,
    )
  }

  private async request(url: URL, init: RequestInit): Promise<Response> {
    try {
      await this.beforeRequest()
      return await this.fetcher(url, {
        ...init,
        signal: AbortSignal.timeout(30_000),
        redirect: 'error',
      })
    } catch {
      // Never propagate fetch's URL, cause or provider body: login contains secrets.
      throw new Error('[eveler] Provider request failed or timed out.')
    }
  }

  private async json(response: Response): Promise<unknown> {
    if (!response.ok) {
      throw new Error(`[eveler] Provider HTTP ${response.status}.`)
    }
    try {
      return await response.json()
    } catch {
      throw new Error('[eveler] Provider response is not valid JSON.')
    }
  }

  private async authenticate(): Promise<string> {
    if (this.token && this.expiresAt > this.now().getTime()) {
      return this.token
    }
    if (!this.identifier || !this.secret) {
      throw new Error(
        '[eveler] Missing EVELER_API_IDENTIFIER or EVELER_API_SECRET.',
      )
    }
    const url = new URL('/api/client/auth/login', origin)
    url.searchParams.set('token', this.identifier)
    url.searchParams.set('secret', this.secret)
    const response = await this.json(await this.request(url, {method: 'POST'}))
    if (
      !isRecord(response) ||
      response.success !== true ||
      !isRecord(response.data) ||
      typeof response.data.token !== 'string' ||
      !response.data.token
    ) {
      throw new Error('[eveler] Invalid authentication response.')
    }
    this.token = response.data.token
    this.expiresAt = this.now().getTime() + 55 * 60_000
    return this.token
  }

  async fetchVolume(
    sourcePointId: string,
    startDate: Date,
    endDate: Date,
    sourceMeterId: string,
    persistRaw?: (body: unknown) => Promise<void>,
  ): Promise<EvelerResponse> {
    if (!/^[A-Za-z0-9_\-]{1,100}$/v.test(sourcePointId)) {
      throw new Error('[eveler] Invalid sourcePointId.')
    }
    if (!/^[a-f0-9]{24}$/v.test(sourceMeterId)) {
      throw new Error(
        '[eveler] Invalid sourceMeterId: expected the provider internal meter identity.',
      )
    }
    if (
      !Number.isFinite(startDate.getTime()) ||
      !Number.isFinite(endDate.getTime()) ||
      endDate <= startDate ||
      startDate.getTime() % day !== 0 ||
      endDate.getTime() % day !== 0 ||
      endDate.getTime() - startDate.getTime() > 365 * day
    ) {
      throw new Error(
        '[eveler] Provider request window must not exceed one year.',
      )
    }
    const start = startDate.toISOString().slice(0, 10)
    const end = endDate.toISOString().slice(0, 10)
    const url = new URL(
      `/api/client/meter/${encodeURIComponent(sourcePointId)}/data/volume/${start}/${end}/4`,
      origin,
    )
    let response = await this.request(url, {
      headers: {
        Accept: 'application/json',
        Authorization: await this.authenticate(),
      },
    })
    if (response.status === 401) {
      this.token = undefined
      response = await this.request(url, {
        headers: {
          Accept: 'application/json',
          Authorization: await this.authenticate(),
        },
      })
    }
    const body = await this.json(response)
    await persistRaw?.(body)
    return validateResponse(body, sourceMeterId)
  }
}

export function aggregateEvelerHours(rows: unknown[], window: EvelerWindow) {
  validateWindow(window)
  const report = {
    completeHours: 0,
    incompleteHours: 0,
    invalidRows: 0,
    duplicateRows: 0,
    conflictingRows: 0,
    outOfWindowRows: 0,
  }
  const intervals = new Map<number, number | null>()
  const invalidHours = new Set<number>()
  for (const row of rows) {
    const end = isRecord(row) ? timestamp(row.date) : NaN
    if (!Number.isFinite(end) || end % step !== 0) {
      report.invalidRows++
      if (
        Number.isFinite(end) &&
        end > window.start.getTime() &&
        end <= window.end.getTime()
      ) {
        invalidHours.add(Math.floor((end - 1) / hour) * hour)
      }
      continue
    }
    if (end <= window.start.getTime() || end > window.end.getTime()) {
      report.outOfWindowRows++
      continue
    }
    const value = isRecord(row) ? row.value : undefined
    // Precision=4 is requested upstream. Integer units prevent binary sum drift.
    const scaled = typeof value === 'number' ? Math.round(value * 10_000) : NaN
    const valid =
      typeof value === 'number' &&
      Number.isFinite(value) &&
      value >= 0 &&
      Number.isSafeInteger(scaled) &&
      Math.abs(value * 10_000 - scaled) < 0.000001
    if (!valid) {
      report.invalidRows++
      intervals.set(end, null)
    } else if (intervals.has(end)) {
      if (intervals.get(end) === scaled) {
        report.duplicateRows++
      } else {
        report.conflictingRows++
        intervals.set(end, null)
      }
    } else {
      intervals.set(end, scaled)
    }
  }
  const values: TimeserieValue[] = []
  for (
    let start = window.start.getTime();
    start < window.end.getTime();
    start += hour
  ) {
    const samples = Array.from({length: 6}, (_, index) =>
      intervals.get(start + (index + 1) * step),
    )
    if (
      invalidHours.has(start) ||
      samples.some((sample) => sample === undefined || sample === null)
    ) {
      report.incompleteHours++
      continue
    }
    const sum = (samples as number[]).reduce(
      (total, sample) => total + sample,
      0,
    )
    if (!Number.isSafeInteger(sum)) {
      report.invalidRows++
      report.incompleteHours++
      continue
    }
    values.push({
      date: new Date(start),
      periodStart: new Date(start),
      periodEnd: new Date(start + hour),
      value: sum / 10_000,
    })
  }
  report.completeHours = values.length
  return {values, report}
}

export function evelerWindows(window: EvelerWindow): EvelerWindow[] {
  validateWindow(window)
  const windows: EvelerWindow[] = []
  for (
    let start = window.start.getTime();
    start < window.end.getTime();
    start += 364 * day
  ) {
    windows.push({
      start: new Date(start),
      end: new Date(Math.min(start + 364 * day, window.end.getTime())),
    })
  }
  return windows
}

type FetchResult = {window: EvelerWindow; rows: unknown[]}
type ParsedResult = ReturnType<typeof aggregateEvelerHours> & {
  window: EvelerWindow
}

export class EvelerConnector extends BaseConnector<FetchResult, ParsedResult> {
  constructor(
    private readonly options: {
      client?: EvelerClient
      now?: () => Date
      window?: EvelerWindow
      cacheDirectory?: string
      cacheOnly?: boolean
    } = {},
  ) {
    super('eveler')
  }

  protected async fetch(context: ConnectorRunContext): Promise<FetchResult> {
    const sourceMeterId = context.connectorParameters?.sourceMeterId
    if (
      typeof sourceMeterId !== 'string' ||
      !/^[a-f0-9]{24}$/v.test(sourceMeterId)
    ) {
      throw new Error(
        '[eveler] connectorParameters.sourceMeterId must be the provider internal meter identity (24 hexadecimal characters).',
      )
    }
    const sourceStart = timestamp(context.connectorParameters?.sourceStartDate)
    if (!Number.isFinite(sourceStart)) {
      throw new Error(
        '[eveler] connectorParameters.sourceStartDate must be an ISO timestamp with timezone.',
      )
    }
    const now = (this.options.now?.() ?? new Date()).getTime()
    const earliestHour = Math.ceil(sourceStart / hour) * hour
    const cursor = context.mostRecentAvailableDate?.getTime()
    if (cursor !== undefined && !Number.isFinite(cursor)) {
      throw new Error('[eveler] Invalid resume cursor.')
    }
    const window = this.options.window ?? {
      start: new Date(
        Math.max(
          earliestHour,
          Math.floor(((cursor ?? earliestHour) - 7 * day) / hour) * hour,
        ),
      ),
      end: new Date(Math.floor(now / hour) * hour),
    }
    if (
      window.start.getTime() < earliestHour ||
      window.end.getTime() > Math.floor(now / hour) * hour
    ) {
      throw new Error(
        '[eveler] Requested window precedes the source or includes an open hour.',
      )
    }
    if (window.start.getTime() === window.end.getTime()) {
      return {window, rows: []}
    }
    const rows: unknown[] = []
    let client = this.options.client
    for (const chunk of evelerWindows(window)) {
      const requestStart = new Date(
        Math.floor(chunk.start.getTime() / day) * day,
      )
      // Endpoint end is exclusive; include the sample at the final hour's end.
      const requestEnd = new Date(
        (Math.floor(chunk.end.getTime() / day) + 1) * day,
      )
      const key = createHash('sha256')
        .update(
          JSON.stringify([
            origin,
            context.sourcePointId,
            requestStart.toISOString(),
            requestEnd.toISOString(),
            'volume',
            4,
          ]),
        )
        .digest('hex')
      const cachePath = this.options.cacheDirectory
        ? join(this.options.cacheDirectory, `${key}.json`)
        : undefined
      let body: EvelerResponse | undefined
      if (cachePath) {
        try {
          body = validateResponse(
            JSON.parse(await readFile(cachePath, 'utf8')) as unknown,
            sourceMeterId,
          )
        } catch (error) {
          if (!isRecord(error) || error.code !== 'ENOENT') {
            // eslint-disable-next-line preserve-caught-error -- Raw cache errors may contain private provider data.
            throw new Error('[eveler] Invalid or unreadable raw cache.')
          }
        }
      }
      if (!body) {
        if (this.options.cacheOnly) {
          throw new Error('[eveler] Required raw cache entry is missing.')
        }
        client ??= new EvelerClient()
        body = await client.fetchVolume(
          context.sourcePointId,
          requestStart,
          requestEnd,
          sourceMeterId,
          cachePath && this.options.cacheDirectory
            ? async (raw) => {
                await mkdir(this.options.cacheDirectory!, {
                  recursive: true,
                  mode: 0o700,
                })
                await writeFile(cachePath, JSON.stringify(raw), {
                  flag: 'wx',
                  mode: 0o600,
                })
              }
            : undefined,
        )
      }
      // Keep each row in its own chunk; daily margin overlaps must not create conflicts.
      for (const row of body.data.values) {
        const end = isRecord(row) ? timestamp(row.date) : NaN
        if (
          !Number.isFinite(end) ||
          (end > chunk.start.getTime() && end <= chunk.end.getTime())
        )
          rows.push(row)
      }
    }
    return {window, rows}
  }

  protected async parse(raw: FetchResult): Promise<ParsedResult> {
    if (raw.window.start.getTime() === raw.window.end.getTime()) {
      return {
        window: raw.window,
        values: [],
        report: {
          completeHours: 0,
          incompleteHours: 0,
          invalidRows: 0,
          duplicateRows: 0,
          conflictingRows: 0,
          outOfWindowRows: 0,
        },
      }
    }
    return {...aggregateEvelerHours(raw.rows, raw.window), window: raw.window}
  }

  protected async process(
    parsed: ParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    console.log(`[eveler] ${JSON.stringify(parsed.report)}`)
    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'eveler',
        channel: 'volume',
        timestampMeaning: 'intervalEnd',
        requestedStart: parsed.window.start.toISOString(),
        requestedEnd: parsed.window.end.toISOString(),
        ...parsed.report,
      },
      min_date: parsed.values[0]?.periodStart,
      max_date: parsed.values.at(-1)?.periodEnd,
      metrics: [
        {
          type: MetricType.VOLUME,
          unit: MetricUnit.M3,
          granularity: Granularity.HOUR,
          conflictPolicy: ConflictPolicy.SKIP_CONFLICTING_VALUES,
          values: parsed.values,
        },
      ],
    }
  }
}
