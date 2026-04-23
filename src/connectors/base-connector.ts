import type {
  ConnectorRunContext,
  ConnectorOutput,
  ParsedPointPayload,
} from './types.js'

export abstract class BaseConnector<TRawData, TParsedData> {
  protected constructor(public readonly name: string) {}

  async run(context: ConnectorRunContext): Promise<ConnectorOutput> {
    console.log(
      `[${this.name}] Running connector for source point: ${context.sourcePointId}`,
    )
    const rawData = await this.fetch(context)
    const parsedSourceData = await this.parse(rawData, context)
    const parsedData = await this.process(parsedSourceData, context)

    return {
      connector: this.name,
      serviceAccount: context.serviceAccount,
      sourcePointId: context.sourcePointId,
      generatedAt: new Date().toISOString(),
      data: parsedData,
    }
  }

  protected getMinMaxDates<T>(
    items: T[],
    getDate: (item: T) => Date,
  ): {minDate: string | undefined; maxDate: string | undefined} {
    if (items.length === 0) {
      return {
        minDate: undefined,
        maxDate: undefined,
      }
    }

    const dates = items.map((item) => getDate(item).getTime())
    return {
      minDate: new Date(Math.min(...dates)).toISOString(),
      maxDate: new Date(Math.max(...dates)).toISOString(),
    }
  }

  protected resolveStartDate(parameters: {
    mostRecentAvailableDate: Date | undefined
    connectorEnabledDate: Date
  }): Date {
    return parameters.mostRecentAvailableDate ?? parameters.connectorEnabledDate
  }

  protected abstract fetch(context: ConnectorRunContext): Promise<TRawData>

  protected abstract parse(
    rawData: TRawData,
    context: ConnectorRunContext,
  ): Promise<TParsedData>

  protected abstract process(
    parsedData: TParsedData,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload>
}
