import {BaseConnector} from './base-connector.js'
import {
  MetricFrequency,
  MetricType,
  MetricUnit,
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
      stationIds: context.pointId,
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
    if (!isWillieConsumptionResponse(rawData)) {
      return {
        id_point_de_prelevement: context.pointId,
        metrics: [],
      }
    }

    const station = rawData.stations.find(
      (item) => item.stationID === context.pointId,
    )
    const datapoints = station?.datapoints ?? []

    return {
      id_point_de_prelevement: context.pointId,
      metrics: [
        {
          type: MetricType.VOLUME_PRELEVE,
          frequency: MetricFrequency.DAY,
          values: datapoints.map((datapoint) => ({
            date: datapoint.dateTime,
            value: datapoint.consumption,
          })),
          unit: MetricUnit.M3,
        },
      ],
    }
  }

  private getStartDate(lastRunAt: string | null | undefined): string {
    if (lastRunAt) {
      return new Date(lastRunAt).toISOString()
    }

    const now = new Date()
    now.setDate(now.getDate() - 1)
    return now.toISOString()
  }
}
