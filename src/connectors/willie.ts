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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isWillieRawDatapoint(value: unknown): value is WillieRawDatapoint {
  return (
    isRecord(value) &&
    typeof value.dateTime === 'string' &&
    typeof value.consumption === 'number'
  )
}

function isWillieRawStation(value: unknown): value is WillieRawStation {
  return (
    isRecord(value) &&
    typeof value.stationID === 'string' &&
    Array.isArray(value.datapoints) &&
    value.datapoints.every((datapoint) => isWillieRawDatapoint(datapoint))
  )
}

function isWillieRawConsumptionResponse(
  value: unknown,
): value is WillieRawConsumptionResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.stations) &&
    value.stations.every((station) => isWillieRawStation(station)) &&
    typeof value.count === 'number' &&
    Array.isArray(value.unknownStationIds) &&
    value.unknownStationIds.every((item) => typeof item === 'string')
  )
}

export class WillieConnector extends BaseConnector {
  private static readonly endpoint =
    'https://api.meetwillie.com/v1/stations/consumption'

  constructor() {
    super('willie')
  }

  protected async fetchSourceData(
    context: ConnectorRunContext,
  ): Promise<unknown> {
    const apiToken = process.env.WILLIE_API_TOKEN
    if (!apiToken) {
      throw new Error(
        `[${this.name}] Missing WILLIE_API_TOKEN environment variable.`,
      )
    }

    const query = new URLSearchParams({
      stationIds: context.sourcePointId,
      startDate: this.getStartDate(context.lastRunAt),
      endDate: new Date().toISOString(),
      resolution: 'day',
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
  ): Promise<ParsedPointPayload> {
    // 1) Valider strictement la shape de la reponse brute Willie.
    if (!isWillieRawConsumptionResponse(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Willie response format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    const response = this.normalizeResponse(rawData, context)

    // 2) Isoler la station cible (point PLE) dans la reponse (il n'y en a qu'une normalement).
    const station = this.getStationForPoint(response, context)
    const {datapoints} = station

    // 3) Construire les metadonnees temporelles globales du lot.
    const {minDate, maxDate} = this.getMinMaxDates(
      datapoints,
      (datapoint) => datapoint.dateTime,
    )

    // 4) Mapper les datapoints Willie vers le contrat metrique PLE.
    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'willie',
        endpoint: WillieConnector.endpoint,
        resolution: 'day',
        station_id: station.stationID,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: MetricType.VOLUME_PRELEVE,
          granularity: Granularity.DAY,
          values: this.mapDatapointsToMetricValues(datapoints),
          unit: MetricUnit.M3,
        },
      ],
    }
  }

  /**
   * Retourne la station correspondant au point PLE.
   * On fail-fast si la station n'est pas presente dans la reponse Willie.
   */
  private getStationForPoint(
    response: WillieConsumptionResponse,
    context: ConnectorRunContext,
  ): WillieStation {
    const station = response.stations.find(
      (item) => item.stationID === context.sourcePointId,
    )
    if (!station) {
      throw new Error(
        `[${this.name}] Station "${context.sourcePointId}" not found in Willie response for service account "${context.serviceAccount}".`,
      )
    }

    return station
  }

  /**
   * Mappe les datapoints Willie vers le format `TimeserieValue` du contrat PLE.
   */
  private mapDatapointsToMetricValues(datapoints: WillieDatapoint[]): Array<{
    date: string
    value: number
  }> {
    return datapoints.map((datapoint) => ({
      date: datapoint.dateTime.toISOString(),
      value: datapoint.consumption,
    }))
  }

  private normalizeResponse(
    response: WillieRawConsumptionResponse,
    context: ConnectorRunContext,
  ): WillieConsumptionResponse {
    const stations = response.stations.map((station) => ({
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
      count: response.count,
      unknownStationIds: response.unknownStationIds,
    }
  }

  private getStartDate(lastRunAt: string | undefined): string {
    if (lastRunAt) {
      return new Date(lastRunAt).toISOString()
    }

    const now = new Date()
    now.setDate(now.getDate() - 1)
    return now.toISOString()
  }
}
