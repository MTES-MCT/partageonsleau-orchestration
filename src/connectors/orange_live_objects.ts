import {BaseConnector} from './base-connector.js'
import {
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'

type OrangeLiveObjectsRecord = {
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
  return typeof value === 'object' && value !== null
}

function isOrangeLiveObjectsRecord(
  value: unknown,
): value is OrangeLiveObjectsRecord {
  if (!isObjectRecord(value)) {
    return false
  }

  if (
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

    return typeof sensor.sensorValue === 'number'
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
  private static readonly metric = {
    type: MetricType.INDEX,
    granularity: Granularity.FIFTEEN_MINUTES,
    unit: MetricUnit.M3,
  } as const

  constructor() {
    super('orange_live_objects')
  }

  protected async fetch(context: ConnectorRunContext): Promise<unknown> {
    const apiKey = process.env.ORANGE_LIVE_OBJECTS_API_KEY
    if (!apiKey) {
      throw new Error(
        `[${this.name}] Missing ORANGE_LIVE_OBJECTS_API_KEY environment variable.`,
      )
    }

    const startDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: OrangeLiveObjectsConnector.connectorEnabledDate,
    })
    const endDate = new Date().toISOString()
    const query = new URLSearchParams({
      limit: '500',
      timeRange: `${startDate.toISOString()},${endDate}`,
    })
    const streamId = encodeURIComponent(context.sourcePointId)
    const url = `${OrangeLiveObjectsConnector.endpoint}/${streamId}?${query}`

    const response = await fetch(url, {
      headers: {
        Accept: 'application/json',
        'X-API-Key': apiKey,
      },
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(
        `[${this.name}] Orange Live Objects API request failed with status ${response.status}: ${body}`,
      )
    }

    return response.json()
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

    return {
      values: this.mapOrangeRecordsToMetricValues(rawData),
    }
  }

  protected async process(
    parsedData: OrangeSourceData,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const {minDate, maxDate} = this.getMinMaxDates(
      parsedData.values,
      (value) => value.date,
    )
    const serializedValues = parsedData.values.map((value) => ({
      date: value.date,
      value: value.value,
    }))

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
          values: serializedValues,
          unit: OrangeLiveObjectsConnector.metric.unit,
        },
      ],
    }
  }

  private mapOrangeRecordsToMetricValues(
    records: OrangeLiveObjectsRecord[],
  ): OrangeMetricValue[] {
    return records.flatMap((record) => {
      const sensorValue = record.value.genericSensor?.['1']?.sensorValue
      if (typeof sensorValue !== 'number') {
        return []
      }

      const date = new Date(record.timestamp)
      if (Number.isNaN(date.getTime())) {
        return []
      }

      return [
        {
          date,
          value: sensorValue,
        },
      ]
    })
  }
}
