import { BaseConnector } from "./base-connector.js";
import {
  MetricFrequency,
  MetricType,
  MetricUnit,
  type ConnectorRunContext,
  type ParsedPointPayload,
} from "./types.js";

type WillieDatapoint = {
  dateTime: string;
  consumption: number;
};

type WillieStation = {
  stationID: string;
  datapoints: WillieDatapoint[];
};

type WillieConsumptionResponse =
  | {
      stations: WillieStation[];
      count: number;
      unknownStationIds: string[];
    }
  | Record<string, unknown>;

export class WillieConnector extends BaseConnector {
  private static readonly endpoint =
    "https://api.meetwillie.com/v1/stations/consumption";

  constructor() {
    super("willie");
  }

  protected async fetchSourceData(
    context: ConnectorRunContext,
  ): Promise<unknown> {
    const apiToken = process.env.WILLIE_API_TOKEN;
    if (!apiToken) {
      throw new Error(`[${this.name}] Missing WILLIE_API_TOKEN environment variable.`);
    }

    const query = new URLSearchParams({
      stationIds: context.pointId,
      startDate: this.getStartDate(context.lastRunAt),
      endDate: new Date().toISOString(),
      resolution: "day",
    });

    const response = await fetch(`${WillieConnector.endpoint}?${query}`, {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${apiToken}`,
      },
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `[${this.name}] Willie API request failed with status ${response.status}: ${body}`,
      );
    }

    return (await response.json()) as WillieConsumptionResponse;
  }

  protected async parse(
    rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const typedData = rawData as WillieConsumptionResponse;
    if (
      !typedData ||
      typeof typedData !== "object" ||
      !("stations" in typedData) ||
      !Array.isArray(typedData.stations)
    ) {
      return {
        id_point_de_prelevement: context.pointId,
        metrics: [],
      };
    }

    const stations = typedData.stations as WillieStation[];
    const station = stations.find((item) => item.stationID === context.pointId);
    const datapoints = station?.datapoints ?? [];

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
    };
  }

  private getStartDate(lastRunAt: string | null): string {
    if (lastRunAt) {
      return new Date(lastRunAt).toISOString();
    }

    const now = new Date();
    now.setDate(now.getDate() - 1);
    return now.toISOString();
  }
}
