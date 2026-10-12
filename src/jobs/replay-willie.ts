import {createHash} from 'node:crypto'
import {readFile} from 'node:fs/promises'
import path from 'node:path'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  PointFlowType,
  SourceType,
  type ConnectorOutput,
} from '../connectors/types.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

type ReplayClient = Pick<
  PartageonsLeauClient,
  'getServiceAccountToken' | 'getContextsForDeclarant' | 'ingest'
>
type Environment = 'prod' | 'testing'
export type WillieReplayWindow = {start: string; end: string}
type Target = {
  environment: Environment
  apiBaseUrl: string
  serviceAccount: string
}
type Binding = {
  declarantId: string
  contextId: string
  pointId: string
  exploitationId: string
  countingCode?: string
  connectorId: string
  sourcePointId: string
  connectorRate: number
  flowType: PointFlowType
}
type FrozenOutput = Omit<ConnectorOutput, 'lastRunAt' | 'data'> & {
  lastRunAt: string
  data: Omit<ConnectorOutput['data'], 'min_date' | 'max_date' | 'metrics'> & {
    min_date?: string
    max_date?: string
    metrics: Array<
      Omit<ConnectorOutput['data']['metrics'][number], 'values'> & {
        values: Array<{
          date: string
          periodStart: string
          periodEnd: string
          value: number
        }>
      }
    >
  }
}
export type WillieReplayPlan = {
  version: 1
  kind: 'willie-replay-plan'
  createdAt: string
  target: Target
  binding: Binding
  window: WillieReplayWindow
  cache: {stationId: string; bodySha256: string; metadataSha256: string}
  summary: {numericDays: number; nullDays: number; outsideWindow: number}
  output: FrozenOutput
  planHash: string
}

const UUID = /^[a-f\d]{8}(?:-[a-f\d]{4}){3}-[a-f\d]{12}$/iv
const SHA256 = /^[a-f\d]{64}$/v
const DAY = 86_400_000
const MAX_CACHE_BYTES = 2 * 1024 * 1024
const endpoint = 'https://api.meetwillie.com/v1/stations/consumption'

function requireCondition(condition: unknown, code: string): asserts condition {
  if (!condition) throw new Error(code)
}

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical)
  if (record(value)) {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right, 'en'))
        .map(([key, child]) => [key, canonical(child)]),
    )
  }
  return value
}

export const willieReplayDigest = (value: unknown): string =>
  createHash('sha256')
    .update(JSON.stringify(canonical(value)))
    .digest('hex')
const byteDigest = (value: Buffer): string =>
  createHash('sha256').update(value).digest('hex')

function dayTimestamp(day: string): number {
  requireCondition(
    /^\d{4}-\d{2}-\d{2}$/v.test(day),
    'WILLIE_REPLAY_DAY_INVALID',
  )
  const value = Date.parse(`${day}T00:00:00Z`)
  requireCondition(
    Number.isFinite(value) &&
      new Date(value).toISOString().slice(0, 10) === day,
    'WILLIE_REPLAY_DAY_INVALID',
  )
  return value
}

/** Explicit French civil days, including 23/25-hour daylight-saving days. */
export function willieParisMidnight(day: string): Date {
  const midnight = dayTimestamp(day)
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Paris',
    timeZoneName: 'shortOffset',
  }).formatToParts(new Date(midnight))
  const offset = parts.find((part) => part.type === 'timeZoneName')?.value
  const match = /^GMT\+(\d{1,2})$/v.exec(offset ?? '')
  requireCondition(match, 'WILLIE_REPLAY_TIMEZONE_INVALID')
  return new Date(midnight - Number(match[1]) * 3_600_000)
}

function nextDay(day: string): string {
  return new Date(dayTimestamp(day) + DAY).toISOString().slice(0, 10)
}

function validateWindow(window: WillieReplayWindow, now: Date): void {
  const start = dayTimestamp(window.start)
  const end = dayTimestamp(window.end)
  requireCondition(
    start >= Date.UTC(2000, 0, 1) && end > start && end - start <= 366 * DAY,
    'WILLIE_REPLAY_WINDOW_INVALID',
  )
  requireCondition(
    willieParisMidnight(window.end).getTime() <= now.getTime(),
    'WILLIE_REPLAY_INCOMPLETE_DAY',
  )
}

function configuredTarget(environment: Environment): Target {
  requireCondition(
    ['prod', 'testing'].includes(environment) &&
      process.env.PLE_BASE_URL &&
      process.env.CLIENT_ID &&
      process.env.CLIENT_SECRET,
    'WILLIE_REPLAY_CONFIGURATION_REQUIRED',
  )
  const url = new URL(process.env.PLE_BASE_URL)
  requireCondition(
    url.protocol === 'https:' &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash,
    'WILLIE_REPLAY_TARGET_INVALID',
  )
  return {
    environment,
    apiBaseUrl: url.toString().replace(/\/$/v, ''),
    serviceAccount: process.env.CLIENT_ID,
  }
}

async function authorizedBinding(
  client: ReplayClient,
  target: Target,
  declarantId: string,
  connectorId: string,
) {
  requireCondition(
    UUID.test(declarantId) && UUID.test(connectorId),
    'WILLIE_REPLAY_IDENTITY_INVALID',
  )
  const token = await client.getServiceAccountToken(target.serviceAccount)
  const contexts = await client.getContextsForDeclarant(declarantId, token)
  const matches = contexts.flatMap((context) =>
    context.points
      .filter((point) => point.connectorId === connectorId)
      .map((point) => ({point, context})),
  )
  requireCondition(matches.length === 1, 'WILLIE_REPLAY_CONNECTOR_AMBIGUOUS')
  const {point, context} = matches[0]
  requireCondition(
    point.connector === 'willie' &&
      UUID.test(point.pointId) &&
      point.exploitationId &&
      UUID.test(point.exploitationId) &&
      UUID.test(point.sourcePointId) &&
      Number.isFinite(point.connectorRate) &&
      point.connectorRate > 0 &&
      point.connectorRate <= 100 &&
      Object.values(PointFlowType).includes(point.flowType as PointFlowType),
    'WILLIE_REPLAY_CONNECTOR_INVALID',
  )
  const binding: Binding = {
    declarantId,
    contextId: context.contextId,
    pointId: point.pointId,
    exploitationId: point.exploitationId,
    ...(point.countingCode !== undefined && {countingCode: point.countingCode}),
    connectorId,
    sourcePointId: point.sourcePointId,
    connectorRate: point.connectorRate,
    flowType: point.flowType as PointFlowType,
  }
  return {binding, token}
}

async function cachedResponse(
  cacheDirectory: string,
  stationId: string,
  window: WillieReplayWindow,
) {
  requireCondition(UUID.test(stationId), 'WILLIE_REPLAY_STATION_INVALID')
  const rawFile = `${stationId}.response.json`
  const metadataFile = `${stationId}.metadata.json`
  const [raw, metadataBytes] = await Promise.all([
    readFile(path.join(cacheDirectory, rawFile)),
    readFile(path.join(cacheDirectory, metadataFile)),
  ])
  requireCondition(
    raw.length <= MAX_CACHE_BYTES && metadataBytes.length <= MAX_CACHE_BYTES,
    'WILLIE_REPLAY_CACHE_TOO_LARGE',
  )
  const metadata: unknown = JSON.parse(metadataBytes.toString('utf8'))
  requireCondition(
    record(metadata) &&
      metadata.version === 1 &&
      metadata.provider === 'willie' &&
      metadata.endpoint === endpoint &&
      metadata.method === 'GET' &&
      metadata.resolution === 'day' &&
      metadata.httpStatus === 200 &&
      metadata.rawFile === rawFile &&
      metadata.bodySha256 === byteDigest(raw) &&
      record(metadata.summary) &&
      metadata.summary.requestedStationId === stationId &&
      record(metadata.window) &&
      metadata.window.timeZone === 'Europe/Paris' &&
      typeof metadata.window.startLocalDate === 'string' &&
      typeof metadata.window.endLocalDateExclusive === 'string',
    'WILLIE_REPLAY_CACHE_INVALID',
  )
  requireCondition(
    dayTimestamp(metadata.window.startLocalDate) <=
      dayTimestamp(window.start) &&
      dayTimestamp(metadata.window.endLocalDateExclusive) >=
        dayTimestamp(window.end),
    'WILLIE_REPLAY_CACHE_WINDOW_MISMATCH',
  )
  const body: unknown = JSON.parse(raw.toString('utf8'))
  return {
    body,
    cache: {
      stationId,
      bodySha256: byteDigest(raw),
      metadataSha256: byteDigest(metadataBytes),
    },
  }
}

/** Pure parser: no provider fetch, API call or hidden cursor/date inference. */
export function parseCachedWillieDays(
  body: unknown,
  stationId: string,
  window: WillieReplayWindow,
) {
  requireCondition(
    record(body) &&
      Array.isArray(body.stations) &&
      Array.isArray(body.unknownStationIds),
    'WILLIE_REPLAY_RESPONSE_INVALID',
  )
  requireCondition(
    !body.unknownStationIds.includes(stationId),
    'WILLIE_REPLAY_STATION_UNKNOWN',
  )
  const stations = body.stations.filter(
    (station: unknown) => record(station) && station.stationID === stationId,
  ) as Array<Record<string, unknown>>
  requireCondition(
    stations.length === 1 &&
      body.stations.length === 1 &&
      Array.isArray(stations[0].datapoints),
    'WILLIE_REPLAY_RESPONSE_STATION_MISMATCH',
  )
  const values: FrozenOutput['data']['metrics'][number]['values'] = []
  const seen = new Set<string>()
  let nullDays = 0
  let outsideWindow = 0
  for (const datapoint of stations[0].datapoints as unknown[]) {
    requireCondition(
      record(datapoint) && typeof datapoint.dateTime === 'string',
      'WILLIE_REPLAY_DATAPOINT_INVALID',
    )
    dayTimestamp(datapoint.dateTime)
    requireCondition(
      !seen.has(datapoint.dateTime),
      'WILLIE_REPLAY_DUPLICATE_DAY',
    )
    seen.add(datapoint.dateTime)
    if (datapoint.dateTime < window.start || datapoint.dateTime >= window.end) {
      outsideWindow++
      continue
    }
    if (datapoint.consumption === null) {
      nullDays++
      continue
    }
    requireCondition(
      typeof datapoint.consumption === 'number' &&
        Number.isFinite(datapoint.consumption) &&
        datapoint.consumption >= 0,
      'WILLIE_REPLAY_CONSUMPTION_INVALID',
    )
    // Willie returns a calendar-day label. Preserve the historical ingestion
    // convention (UTC midnight), rather than shifting existing daily periods.
    const periodStart = new Date(dayTimestamp(datapoint.dateTime)).toISOString()
    values.push({
      date: periodStart,
      periodStart,
      periodEnd: new Date(
        dayTimestamp(nextDay(datapoint.dateTime)),
      ).toISOString(),
      value: datapoint.consumption,
    })
  }
  values.sort((left, right) => left.date.localeCompare(right.date, 'en'))
  return {
    values,
    summary: {numericDays: values.length, nullDays, outsideWindow},
  }
}

function frozenOutput(
  target: Target,
  binding: Binding,
  window: WillieReplayWindow,
  cache: WillieReplayPlan['cache'],
  body: unknown,
  createdAt: string,
) {
  const {values, summary} = parseCachedWillieDays(
    body,
    binding.sourcePointId,
    window,
  )
  const output: FrozenOutput = {
    connector: 'willie',
    exploitationId: binding.exploitationId,
    ...(binding.countingCode !== undefined && {
      countingCode: binding.countingCode,
    }),
    serviceAccount: target.serviceAccount,
    sourcePointId: binding.sourcePointId,
    connectorId: binding.connectorId,
    connectorRate: binding.connectorRate,
    lastRunAt: createdAt,
    data: {
      id_point_de_prelevement: binding.sourcePointId,
      exploitationId: binding.exploitationId,
      ...(binding.countingCode !== undefined && {
        countingCode: binding.countingCode,
      }),
      flow_type: binding.flowType,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'willie',
        endpoint,
        resolution: 'day',
        station_id: binding.sourcePointId,
        replay: {
          bodySha256: cache.bodySha256,
          window,
          providerWindowTimeZone: 'Europe/Paris',
          periodConvention: 'UTC_CALENDAR_DAY',
        },
      },
      min_date: values[0]?.periodStart,
      max_date: values.at(-1)?.periodEnd,
      metrics: [
        {
          type: MetricType.VOLUME,
          granularity: Granularity.DAY,
          conflictPolicy: ConflictPolicy.SKIP_CONFLICTING_VALUES,
          unit: MetricUnit.M3,
          exploitationId: binding.exploitationId,
          ...(binding.countingCode !== undefined && {
            countingCode: binding.countingCode,
          }),
          values,
        },
      ],
    },
  }
  return {output, summary}
}

export async function prepareWillieReplay(
  options: {
    target: Environment
    connectorId: string
    declarantId: string
    window: WillieReplayWindow
    cacheDirectory: string
  },
  client: ReplayClient = new PartageonsLeauClient(),
  now = new Date(),
): Promise<WillieReplayPlan> {
  validateWindow(options.window, now)
  const target = configuredTarget(options.target)
  const {binding} = await authorizedBinding(
    client,
    target,
    options.declarantId,
    options.connectorId,
  )
  const {body, cache} = await cachedResponse(
    options.cacheDirectory,
    binding.sourcePointId,
    options.window,
  )
  const createdAt = now.toISOString()
  const {output, summary} = frozenOutput(
    target,
    binding,
    options.window,
    cache,
    body,
    createdAt,
  )
  const payload = {
    version: 1 as const,
    kind: 'willie-replay-plan' as const,
    createdAt,
    target,
    binding,
    window: {...options.window},
    cache,
    summary,
    output,
  }
  return {...payload, planHash: willieReplayDigest(payload)}
}

export function validateWillieReplayPlan(
  value: unknown,
  expectedHash: string,
): WillieReplayPlan {
  requireCondition(record(value), 'WILLIE_REPLAY_PLAN_INVALID')
  const {planHash, ...payload} = value
  requireCondition(
    SHA256.test(expectedHash) &&
      planHash === expectedHash &&
      willieReplayDigest(payload) === expectedHash,
    'WILLIE_REPLAY_PLAN_HASH_MISMATCH',
  )
  requireCondition(
    value.version === 1 &&
      value.kind === 'willie-replay-plan' &&
      typeof value.createdAt === 'string' &&
      Number.isFinite(Date.parse(value.createdAt)) &&
      record(value.target) &&
      record(value.binding) &&
      record(value.cache) &&
      record(value.window) &&
      record(value.summary) &&
      record(value.output) &&
      typeof value.window.start === 'string' &&
      typeof value.window.end === 'string' &&
      typeof value.binding.declarantId === 'string' &&
      typeof value.binding.connectorId === 'string',
    'WILLIE_REPLAY_PLAN_INVALID',
  )
  return value as WillieReplayPlan
}

function thawOutput(output: FrozenOutput): ConnectorOutput {
  return {
    ...output,
    lastRunAt: new Date(output.lastRunAt),
    data: {
      ...output.data,
      min_date: output.data.min_date
        ? new Date(output.data.min_date)
        : undefined,
      max_date: output.data.max_date
        ? new Date(output.data.max_date)
        : undefined,
      metrics: output.data.metrics.map((metric) => ({
        ...metric,
        values: metric.values.map((value) => ({
          ...value,
          date: new Date(value.date),
          periodStart: new Date(value.periodStart),
          periodEnd: new Date(value.periodEnd),
        })),
      })),
    },
  }
}

/** Applies only a reviewed, byte-identical cached payload; never calls Willie. */
export async function applyWillieReplay(
  options: {
    target: Environment
    plan: unknown
    expectedPlanHash: string
    cacheDirectory: string
  },
  client: ReplayClient = new PartageonsLeauClient(),
  now = new Date(),
) {
  const plan = validateWillieReplayPlan(options.plan, options.expectedPlanHash)
  validateWindow(plan.window, now)
  const target = configuredTarget(options.target)
  requireCondition(
    willieReplayDigest(target) === willieReplayDigest(plan.target),
    'WILLIE_REPLAY_TARGET_CHANGED',
  )
  const {binding, token} = await authorizedBinding(
    client,
    target,
    plan.binding.declarantId,
    plan.binding.connectorId,
  )
  requireCondition(
    willieReplayDigest(binding) === willieReplayDigest(plan.binding),
    'WILLIE_REPLAY_MAPPING_CHANGED',
  )
  const {body, cache} = await cachedResponse(
    options.cacheDirectory,
    binding.sourcePointId,
    plan.window,
  )
  requireCondition(
    willieReplayDigest(cache) === willieReplayDigest(plan.cache),
    'WILLIE_REPLAY_CACHE_CHANGED',
  )
  const rebuilt = frozenOutput(
    target,
    binding,
    plan.window,
    cache,
    body,
    plan.createdAt,
  )
  requireCondition(
    willieReplayDigest(rebuilt.output) === willieReplayDigest(plan.output) &&
      willieReplayDigest(rebuilt.summary) === willieReplayDigest(plan.summary),
    'WILLIE_REPLAY_PAYLOAD_CHANGED',
  )
  requireCondition(
    plan.summary.numericDays > 0,
    'WILLIE_REPLAY_NO_NUMERIC_VALUES',
  )
  const acknowledgement = await client.ingest({
    output: thawOutput(plan.output),
    pointId: binding.pointId,
    declarantId: binding.declarantId,
    contextId: binding.contextId,
    serviceAccountToken: token,
    requireAcknowledgement: true,
  })
  requireCondition(
    acknowledgement &&
      typeof acknowledgement.imported === 'boolean' &&
      Number.isSafeInteger(acknowledgement.skippedValues) &&
      acknowledgement.skippedValues >= 0 &&
      acknowledgement.skippedValues <= plan.summary.numericDays &&
      (acknowledgement.imported
        ? acknowledgement.skippedValues < plan.summary.numericDays
        : acknowledgement.skippedValues === plan.summary.numericDays),
    'WILLIE_REPLAY_ACKNOWLEDGEMENT_INVALID',
  )
  return {
    version: 1,
    kind: 'willie-replay-receipt',
    planHash: plan.planHash,
    target: plan.target,
    binding: plan.binding,
    window: plan.window,
    appliedAt: now.toISOString(),
    acknowledgement,
    numericDays: plan.summary.numericDays,
  }
}
