import {BaseConnector} from './base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'

type OrangeLiveObjectsRecord = {
  id: string
  streamId: string
  timestamp: string
  value: {
    genericSensor?: Record<string, {sensorValue?: number}>
  }
}

type OrangeMetricValue = {
  date: Date
  value: number
}

type OrangeSourceData = {
  values: OrangeMetricValue[]
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isOrangeLiveObjectsRecord(
  value: unknown,
): value is OrangeLiveObjectsRecord {
  if (!isObjectRecord(value)) {
    return false
  }

  if (
    typeof value.id !== 'string' ||
    value.id.length === 0 ||
    typeof value.streamId !== 'string' ||
    typeof value.timestamp !== 'string'
  ) {
    return false
  }

  if (!isObjectRecord(value.value)) {
    return false
  }

  if (value.value.genericSensor === undefined) {
    return true
  }

  if (!isObjectRecord(value.value.genericSensor)) {
    return false
  }

  return Object.values(value.value.genericSensor).every((sensor) => {
    if (!isObjectRecord(sensor)) {
      return false
    }

    if (sensor.sensorValue === undefined) {
      return true
    }

    return (
      typeof sensor.sensorValue === 'number' &&
      Number.isFinite(sensor.sensorValue)
    )
  })
}

function isOrangeLiveObjectsResponse(
  value: unknown,
): value is OrangeLiveObjectsRecord[] {
  return (
    Array.isArray(value) &&
    value.every((record) => isOrangeLiveObjectsRecord(record))
  )
}

export class OrangeLiveObjectsConnector extends BaseConnector<
  unknown,
  OrangeSourceData
> {
  private static readonly endpoint =
    'https://liveobjects.orange-business.com/api/v0/data/streams'

  private static readonly connectorEnabledDate = new Date('2026-01-01')
  private static readonly pageSize = 500
  private static readonly maxPages = 1000
  private static readonly metric = {
    type: MetricType.INDEX,
    granularity: Granularity.FIFTEEN_MINUTES,
    conflictPolicy: ConflictPolicy.REPLACE_EXISTING,
    unit: MetricUnit.M3,
  } as const

  constructor(
    private readonly options: {
      apiKey?: string
      fetcher?: typeof fetch
      now?: () => Date
    } = {},
  ) {
    super('orange_live_objects')
  }

  protected async fetch(context: ConnectorRunContext): Promise<unknown> {
    const apiKey =
      this.options.apiKey ?? process.env.ORANGE_LIVE_OBJECTS_API_KEY

    if (!apiKey) {
      throw new Error(
        `[${this.name}] Missing ORANGE_LIVE_OBJECTS_API_KEY environment variable.`,
      )
    }

    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: OrangeLiveObjectsConnector.connectorEnabledDate,
    })

    const endDate = (this.options.now?.() ?? new Date()).toISOString()
    if (!Number.isFinite(startDate.getTime())) {
      throw new Error(`[${this.name}] Invalid resume date.`)
    }
    if (startDate.getTime() >= Date.parse(endDate)) return []

    const query = new URLSearchParams({
      limit: String(OrangeLiveObjectsConnector.pageSize),
      timeRange: `${startDate.toISOString()},${endDate}`,
    })

    const streamId = encodeURIComponent(context.sourcePointId)

    console.log(
      `[${this.name}] Fetch stream="${context.sourcePointId}", startDate=${startDate.toISOString()}, endDate=${endDate}`,
    )

    const records: OrangeLiveObjectsRecord[] = []
    const bookmarks = new Set<string>()
    const recordIds = new Map<string, string>()
    for (let page = 0; page < OrangeLiveObjectsConnector.maxPages; page++) {
      const url = `${OrangeLiveObjectsConnector.endpoint}/${streamId}?${query}`
      const payload = await this.fetchPage(url, apiKey)
      if (!isOrangeLiveObjectsResponse(payload)) {
        throw new Error(`[${this.name}] Invalid Orange response format.`)
      }
      // A short page is not proof of exhaustion: always request the next bookmark.
      if (payload.length === 0) return records

      for (const record of payload) {
        const timestamp = Date.parse(record.timestamp)
        if (
          record.streamId !== context.sourcePointId ||
          !Number.isFinite(timestamp) ||
          timestamp < startDate.getTime() ||
          timestamp >= Date.parse(endDate)
        ) {
          throw new Error(`[${this.name}] Invalid stream identity or date.`)
        }
        const signature = JSON.stringify([
          timestamp,
          record.value.genericSensor?.['1']?.sensorValue,
        ])
        const previous = recordIds.get(record.id)
        if (previous !== undefined && previous !== signature) {
          throw new Error(`[${this.name}] Conflicting Orange record ID.`)
        }
        if (previous === undefined) records.push(record)
        recordIds.set(record.id, signature)
      }

      const bookmark = payload[payload.length - 1].id
      if (bookmarks.has(bookmark)) {
        throw new Error(`[${this.name}] Orange pagination did not advance.`)
      }
      bookmarks.add(bookmark)
      query.set('bookmarkId', bookmark)
    }
    throw new Error(`[${this.name}] Orange pagination limit exceeded.`)
  }

  private async fetchPage(url: string, apiKey: string): Promise<unknown> {
    let response: Response
    try {
      response = await (this.options.fetcher ?? fetch)(url, {
        method: 'GET',
        redirect: 'error',
        signal: AbortSignal.timeout(30_000),
        headers: {
          Accept: 'application/json',
          'X-API-Key': apiKey,
        },
      })
    } catch {
      throw new Error(`[${this.name}] Provider request failed or timed out.`)
    }
    if (!response.ok) {
      throw new Error(
        `[${this.name}] Orange request failed: HTTP ${response.status}.`,
      )
    }
    try {
      return (await response.json()) as unknown
    } catch {
      throw new Error(`[${this.name}] Invalid Orange JSON response.`)
    }
  }

  protected async parse(
    rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<OrangeSourceData> {
    if (!isOrangeLiveObjectsResponse(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Orange Live Objects response format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    // Orange returns newest first. Retain the oldest bounded batch so the API's
    // per-connector cursor cannot jump beyond observations not yet ingested.
    const allValues = this.mapOrangeRecordsToMetricValues(rawData)
    let values = allValues.slice(0, OrangeLiveObjectsConnector.pageSize)
    const nextValue = allValues[OrangeLiveObjectsConnector.pageSize]
    if (nextValue) {
      // The API cursor is the end of the normalized 15-minute bucket. Never
      // split that bucket, otherwise the next pull would skip its remainder.
      const nextBucket = Math.floor(nextValue.date.getTime() / 900_000)
      values = values.filter(
        (value) => Math.floor(value.date.getTime() / 900_000) < nextBucket,
      )
      if (values.length === 0) {
        throw new Error(
          `[${this.name}] Too many indexes in one 15-minute bucket.`,
        )
      }
    }

    console.log(
      `[${this.name}] Parsed records=${rawData.length}, values=${values.length}, stream="${context.sourcePointId}"`,
    )

    return {values}
  }

  protected async process(
    parsedData: OrangeSourceData,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const {minDate, maxDate} = this.getMinMaxDates(
      parsedData.values,
      (value) => value.date,
    )

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'orange_live_objects',
        endpoint: OrangeLiveObjectsConnector.endpoint,
        stream_id: context.sourcePointId,
        sensor_index: '1',
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: OrangeLiveObjectsConnector.metric.type,
          granularity: OrangeLiveObjectsConnector.metric.granularity,
          conflictPolicy: OrangeLiveObjectsConnector.metric.conflictPolicy,
          values: parsedData.values,
          unit: OrangeLiveObjectsConnector.metric.unit,
        },
      ],
    }
  }

  private mapOrangeRecordsToMetricValues(
    records: OrangeLiveObjectsRecord[],
  ): OrangeMetricValue[] {
    const values = new Map<number, OrangeMetricValue>()
    for (const record of records) {
      const sensorValue = record.value.genericSensor?.['1']?.sensorValue

      if (typeof sensorValue !== 'number') {
        continue
      }

      const date = new Date(record.timestamp)

      if (!Number.isFinite(sensorValue) || !Number.isFinite(date.getTime())) {
        throw new Error(`[${this.name}] Invalid Orange index or date.`)
      }
      const existing = values.get(date.getTime())
      if (existing && existing.value !== sensorValue) {
        throw new Error(
          `[${this.name}] Conflicting indexes at the same timestamp.`,
        )
      }
      // Retransmissions can have distinct Orange IDs with the same observation.
      values.set(date.getTime(), {date, value: sensorValue})
    }
    return [...values.values()].sort(
      (left, right) => left.date.getTime() - right.date.getTime(),
    )
  }
}
