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
  dateTime: string
  consumption: number
}

type WillieStation = {
  stationID: string
  datapoints: WillieDatapoint[]
}

type WillieConsumptionResponse = {
  stations: WillieStation[]
  count: number
  unknownStationIds: string[]
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isWillieDatapoint(value: unknown): value is WillieDatapoint {
  return (
    isRecord(value) &&
    typeof value.dateTime === 'string' &&
    typeof value.consumption === 'number'
  )
}

function isWillieStation(value: unknown): value is WillieStation {
  return (
    isRecord(value) &&
    typeof value.stationID === 'string' &&
    Array.isArray(value.datapoints) &&
    value.datapoints.every((datapoint) => isWillieDatapoint(datapoint))
  )
}

function isWillieConsumptionResponse(
  value: unknown,
): value is WillieConsumptionResponse {
  return (
    isRecord(value) &&
    Array.isArray(value.stations) &&
    value.stations.every((station) => isWillieStation(station)) &&
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
    if (!isWillieConsumptionResponse(rawData)) {
      throw new Error(
        `[${this.name}] Invalid Willie response format for service account "${context.serviceAccount}" and source point "${context.sourcePointId}".`,
      )
    }

    // 2) Isoler la station cible (point PLE) dans la reponse (il n'y en a qu'une normalement).
    const station = this.getStationForPoint(rawData, context)
    const datapoints = station.datapoints

    // 3) Construire les metadonnees temporelles globales du lot.
    const {minDate, maxDate} = this.getMinMaxDates(datapoints)

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
   * Extrait min/max date a partir des datapoints retournes par Willie.
   * Retourne undefined si la liste est vide.
   */
  private getMinMaxDates(datapoints: WillieDatapoint[]): {
    minDate: string | undefined
    maxDate: string | undefined
  } {
    const dates = datapoints.map((datapoint) => datapoint.dateTime)
    if (dates.length === 0) {
      return {
        minDate: undefined,
        maxDate: undefined,
      }
    }

    return {
      minDate: dates.reduce((currentMin, date) =>
        date < currentMin ? date : currentMin,
      ),
      maxDate: dates.reduce((currentMax, date) =>
        date > currentMax ? date : currentMax,
      ),
    }
  }

  /**
   * Mappe les datapoints Willie vers le format `TimeserieValue` du contrat PLE.
   */
  private mapDatapointsToMetricValues(datapoints: WillieDatapoint[]): Array<{
    date: string
    value: number
  }> {
    return datapoints.map((datapoint) => ({
      date: datapoint.dateTime,
      value: datapoint.consumption,
    }))
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
