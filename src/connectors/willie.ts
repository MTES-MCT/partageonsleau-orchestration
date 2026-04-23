import {BaseConnector} from './base-connector.js'
import {
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from './types.js'

type WillieDatapoint = {
  dateTime: Date
  consumption: number
}

type WillieRawDatapoint = {
  dateTime: string
  consumption: number
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

const willieResolutionByGranularity = {
  [Granularity.HOUR]: 'hour',
  [Granularity.DAY]: 'day',
  [Granularity.WEEK]: 'week',
  [Granularity.MONTH]: 'month',
  [Granularity.YEAR]: 'year',
} as const satisfies Record<
  Exclude<Granularity, Granularity.FIFTEEN_MINUTES>,
  string
>

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isWillieRawDatapoint(value: unknown): value is WillieRawDatapoint {
  return (
    isObjectRecord(value) &&
    typeof value.dateTime === 'string' &&
    typeof value.consumption === 'number'
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
  if (granularity === Granularity.FIFTEEN_MINUTES) {
    throw new Error(
      '[WillieConnector] Granularity "15_minutes" is not supported by Willie API.',
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
  unknown,
  WillieConsumptionResponse
> {
  private static readonly endpoint =
    'https://api.meetwillie.com/v1/stations/consumption'

  private static readonly granularity = Granularity.HOUR
  // Idéalement configurable par clé d'API (incompatible avec le modèle actuel)
  private static readonly connectorEnabledDate = new Date('2026-01-01')

  constructor() {
    super('willie')
  }

  protected async fetch(context: ConnectorRunContext): Promise<unknown> {
    const apiToken = process.env.WILLIE_API_TOKEN
    if (!apiToken) {
      throw new Error(
        `[${this.name}] Missing WILLIE_API_TOKEN environment variable.`,
      )
    }

    const query = new URLSearchParams({
      stationIds: context.sourcePointId,
      startDate: this.resolveStartDate({
        mostRecentAvailableDate: context.mostRecentAvailableDate,
        connectorEnabledDate: WillieConnector.connectorEnabledDate,
      }).toISOString(),
      endDate: new Date().toISOString(),
      resolution: granularityToWillieResolution(WillieConnector.granularity),
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

    return response.json()
  }

  protected async parse(
    rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<WillieConsumptionResponse> {
    if (!isWillieRawConsumptionResponse(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Willie response format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    const stations = rawData.stations.map((station) => ({
      stationID: station.stationID,
      datapoints: station.datapoints
        .map((datapoint) => {
          const dateTime = new Date(datapoint.dateTime)
          if (Number.isNaN(dateTime.getTime())) {
            console.warn(
              `[${this.name}] Invalid Willie datapoint date "${datapoint.dateTime}" for station "${station.stationID}" (service account "${context.serviceAccount}").`,
            )
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

    const {minDate, maxDate} = this.getMinMaxDates(
      datapoints,
      (datapoint) => datapoint.dateTime,
    )

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'willie',
        endpoint: WillieConnector.endpoint,
        resolution: granularityToWillieResolution(WillieConnector.granularity),
        station_id: station.stationID,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: MetricType.VOLUME_PRELEVE,
          granularity: WillieConnector.granularity,
          values: this.mapWillieDatapointsToMetricValues(datapoints),
          unit: MetricUnit.M3,
        },
      ],
    }
  }

  /**
   * Mappe les datapoints Willie vers le format `TimeserieValue` du contrat PLE.
   */
  private mapWillieDatapointsToMetricValues(
    datapoints: WillieDatapoint[],
  ): Array<{
    date: string
    value: number
  }> {
    return datapoints.map((datapoint) => ({
      date: datapoint.dateTime.toISOString(),
      value: datapoint.consumption,
    }))
  }
}
