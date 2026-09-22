import type {
  ConnectorDiscoveryContext,
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
    if (
      (context.exploitationId &&
        parsedData.exploitationId &&
        context.exploitationId !== parsedData.exploitationId) ||
      (context.countingCode &&
        parsedData.countingCode &&
        context.countingCode !== parsedData.countingCode)
    ) {
      throw new Error(
        `[${this.name}] L'identité du comptage ne correspond pas à l'exploitation du connecteur.`,
      )
    }
    const metrics = parsedData.metrics.map((metric) => {
      if (
        (context.exploitationId &&
          metric.exploitationId &&
          context.exploitationId !== metric.exploitationId) ||
        (context.countingCode &&
          metric.countingCode &&
          context.countingCode !== metric.countingCode)
      ) {
        throw new Error(
          `[${this.name}] L'identité du comptage ne correspond pas à l'exploitation du connecteur.`,
        )
      }

      return {
        ...metric,
        ...((metric.exploitationId ?? context.exploitationId) && {
          exploitationId: metric.exploitationId ?? context.exploitationId,
        }),
        ...((metric.countingCode ?? context.countingCode) && {
          countingCode: metric.countingCode ?? context.countingCode,
        }),
      }
    })

    return {
      connector: this.name,
      ...(context.exploitationId && {exploitationId: context.exploitationId}),
      ...(context.countingCode && {countingCode: context.countingCode}),
      serviceAccount: context.serviceAccount,
      sourcePointId: context.sourcePointId,
      connectorId: context.connectorId,
      connectorRate: context.rate,
      lastRunAt: new Date(),
      data: {
        ...parsedData,
        ...(context.exploitationId && {exploitationId: context.exploitationId}),
        ...(context.countingCode && {countingCode: context.countingCode}),
        flow_type: parsedData.flow_type ?? context.flowType,
        metrics,
      },
    }
  }

  async discoverSourcePointIds(
    _context: ConnectorDiscoveryContext,
  ): Promise<string[]> {
    return []
  }

  protected getMinMaxDates<T>(
    items: T[],
    getDate: (item: T) => Date,
  ): {minDate: Date | undefined; maxDate: Date | undefined} {
    if (items.length === 0) {
      return {
        minDate: undefined,
        maxDate: undefined,
      }
    }

    const dates = items.map((item) => getDate(item).getTime())
    return {
      minDate: new Date(Math.min(...dates)),
      maxDate: new Date(Math.max(...dates)),
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
