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

type WillieDatapoint = {
  dateTime: Date
  consumption: number
}

type WillieRawDatapoint = {
  dateTime: string
  consumption: unknown
}

type WillieStation = {
  stationID: string
  datapoints: WillieDatapoint[]
}

type WillieRawStation = {
  stationID: string
  datapoints: WillieRawDatapoint[]
}

type WillieConsumptionResponse = {
  stations: WillieStation[]
  count: number
  unknownStationIds: string[]
}

type WillieRawConsumptionResponse = {
  stations: WillieRawStation[]
  count: number
  unknownStationIds: string[]
}

type WillieFetchedResponse = {
  body: unknown
  completedBeforeDay: string
}

const parisDayFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Paris',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
})

function parisDay(now: Date): string {
  const parts = parisDayFormatter.formatToParts(now)
  const part = (type: string) =>
    parts.find((value) => value.type === type)?.value
  return `${part('year')}-${part('month')}-${part('day')}`
}

const willieResolutionByGranularity = {
  [Granularity.HOUR]: 'hour',
  [Granularity.DAY]: 'day',
  [Granularity.WEEK]: 'week',
  [Granularity.MONTH]: 'month',
  [Granularity.YEAR]: 'year',
} as const satisfies Record<
  Exclude<Granularity, Granularity.FIFTEEN_MINUTES | Granularity.QUARTER>,
  string
>

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isWillieRawDatapoint(value: unknown): value is WillieRawDatapoint {
  return (
    isObjectRecord(value) &&
    typeof value.dateTime === 'string' &&
    (typeof value.consumption === 'number' || value.consumption === null)
  )
}

function isWillieRawStation(value: unknown): value is WillieRawStation {
  return (
    isObjectRecord(value) &&
    typeof value.stationID === 'string' &&
    Array.isArray(value.datapoints) &&
    value.datapoints.every((datapoint) => isWillieRawDatapoint(datapoint))
  )
}

function isWillieRawConsumptionResponse(
  value: unknown,
): value is WillieRawConsumptionResponse {
  return (
    isObjectRecord(value) &&
    Array.isArray(value.stations) &&
    value.stations.every((station) => isWillieRawStation(station)) &&
    typeof value.count === 'number' &&
    Array.isArray(value.unknownStationIds) &&
    value.unknownStationIds.every((item) => typeof item === 'string')
  )
}

function granularityToWillieResolution(granularity: Granularity): string {
  if (
    granularity === Granularity.FIFTEEN_MINUTES ||
    granularity === Granularity.QUARTER
  ) {
    throw new Error(
      `[WillieConnector] Granularity "${granularity}" is not supported by Willie API.`,
    )
  }

  return willieResolutionByGranularity[granularity]
}

/**
 * Retourne la station correspondant au point PLE.
 * On fail-fast si la station n'est pas presente dans la reponse Willie.
 */
function assertAndGetStationForPoint(
  response: WillieConsumptionResponse,
  context: ConnectorRunContext,
): WillieStation {
  const station = response.stations.find(
    (item) => item.stationID === context.sourcePointId,
  )
  if (!station) {
    throw new Error(
      `[willie] Station "${context.sourcePointId}" not found in Willie response for service account "${context.serviceAccount}".`,
    )
  }

  return station
}

export class WillieConnector extends BaseConnector<
  WillieFetchedResponse,
  WillieConsumptionResponse
> {
  private static readonly endpoint =
    'https://api.meetwillie.com/v1/stations/consumption'

  private static readonly metric = {
    type: MetricType.VOLUME,
    granularity: Granularity.DAY,
    conflictPolicy: ConflictPolicy.SKIP_CONFLICTING_VALUES,
    unit: MetricUnit.M3,
  } as const

  // Idéalement configurable par clé d'API (incompatible avec le modèle actuel)
  private static readonly connectorEnabledDate = new Date('2026-01-01')

  constructor(private readonly options: {now?: () => Date} = {}) {
    super('willie')
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<WillieFetchedResponse> {
    const apiToken = process.env.WILLIE_API_TOKEN
    if (!apiToken) {
      throw new Error(
        `[${this.name}] Missing WILLIE_API_TOKEN environment variable.`,
      )
    }

    // Freeze the cutoff before the request: a slow response across midnight
    // must not turn a still-partial provider day into an admissible full day.
    const now = this.options.now?.() ?? new Date()
    const completedBeforeDay = parisDay(now)
    const lastCompletedDay = new Date(`${completedBeforeDay}T00:00:00Z`)
    lastCompletedDay.setUTCDate(lastCompletedDay.getUTCDate() - 1)
    if (lastCompletedDay < WillieConnector.connectorEnabledDate) {
      return {
        body: {
          stations: [{stationID: context.sourcePointId, datapoints: []}],
          count: 0,
          unknownStationIds: [],
        },
        completedBeforeDay,
      }
    }
    const cursor = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: WillieConnector.connectorEnabledDate,
    })
    // Older pulls may have advanced the cursor beyond today after receiving a
    // partial day. Re-read the last closed day without replacing stored values.
    const startDate =
      context.mostRecentAvailableDate && cursor > lastCompletedDay
        ? lastCompletedDay
        : cursor
    const query = new URLSearchParams({
      stationIds: context.sourcePointId,
      startDate: startDate.toISOString(),
      endDate: now.toISOString(),
      resolution: granularityToWillieResolution(
        WillieConnector.metric.granularity,
      ),
    })

    const response = await fetch(`${WillieConnector.endpoint}?${query}`, {
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${apiToken}`,
      },
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(
        `[${this.name}] Willie API request failed with status ${response.status}: ${body}`,
      )
    }

    const body: unknown = await response.json()
    return {body, completedBeforeDay}
  }

  protected async parse(
    fetched: WillieFetchedResponse,
    context: ConnectorRunContext,
  ): Promise<WillieConsumptionResponse> {
    const rawData = fetched.body
    if (!isWillieRawConsumptionResponse(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Willie response format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    const stations = rawData.stations.map((station) => ({
      stationID: station.stationID,
      datapoints: station.datapoints
        .map((datapoint) => {
          if (datapoint.consumption === null) {
            return undefined
          }

          const dateTime = new Date(datapoint.dateTime)
          if (Number.isNaN(dateTime.getTime())) {
            console.warn(
              `[${this.name}] Invalid Willie datapoint date "${datapoint.dateTime}" for station "${station.stationID}" (service account "${context.serviceAccount}").`,
            )
            return undefined
          }
          if (
            dateTime.toISOString().slice(0, 10) >= fetched.completedBeforeDay
          ) {
            return undefined
          }

          return {
            dateTime,
            consumption: datapoint.consumption,
          }
        })
        .filter(
          (datapoint): datapoint is WillieDatapoint => datapoint !== undefined,
        ),
    }))

    const rawDatapointCount = rawData.stations.reduce(
      (count, station) => count + station.datapoints.length,
      0,
    )
    const parsedDatapointCount = stations.reduce(
      (count, station) => count + station.datapoints.length,
      0,
    )
    const droppedDatapointCount = rawDatapointCount - parsedDatapointCount
    if (droppedDatapointCount > 0) {
      console.warn(
        `[${this.name}] Dropped ${droppedDatapointCount} null, invalid or incomplete daily datapoints for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    return {
      stations,
      count: rawData.count,
      unknownStationIds: rawData.unknownStationIds,
    }
  }

  protected async process(
    parsedData: WillieConsumptionResponse,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const station = assertAndGetStationForPoint(parsedData, context)
    const {datapoints} = station
    const values = this.mapWillieDatapointsToMetricValues(datapoints)
    const {minDate} = this.getMinMaxDates(values, (value) => value.periodStart)
    const {maxDate} = this.getMinMaxDates(values, (value) => value.periodEnd)

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'willie',
        endpoint: WillieConnector.endpoint,
        resolution: granularityToWillieResolution(
          WillieConnector.metric.granularity,
        ),
        station_id: station.stationID,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: WillieConnector.metric.type,
          granularity: WillieConnector.metric.granularity,
          conflictPolicy: WillieConnector.metric.conflictPolicy,
          values,
          unit: WillieConnector.metric.unit,
        },
      ],
    }
  }

  /**
   * Mappe les datapoints Willie vers le format `TimeserieValue` du contrat PLE.
   */
  private mapWillieDatapointsToMetricValues(
    datapoints: WillieDatapoint[],
  ): Array<TimeserieValue & {periodStart: Date; periodEnd: Date}> {
    return datapoints.map((datapoint) => {
      // Willie labels civil days as YYYY-MM-DD. Preserve the historical API
      // representation [UTC midnight, next UTC midnight), including DST days.
      // Explicit periods also make the ingestion acknowledgement use the same
      // bounds as the API instead of treating the last day's start as its end.
      const periodStart = new Date(datapoint.dateTime)
      periodStart.setUTCHours(0, 0, 0, 0)
      const periodEnd = new Date(periodStart)
      periodEnd.setUTCDate(periodEnd.getUTCDate() + 1)
      return {
        date: periodStart,
        periodStart,
        periodEnd,
        value: datapoint.consumption,
      }
    })
  }
}
