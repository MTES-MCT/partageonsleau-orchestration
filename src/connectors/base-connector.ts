import type {
  ConnectorRunContext,
  ConnectorOutput,
  ParsedPointPayload,
} from './types.js'

export abstract class BaseConnector {
  protected constructor(public readonly name: string) {}

  async run(context: ConnectorRunContext): Promise<ConnectorOutput> {
    console.log(
      `[${this.name}] Running connector for point: ${context.pointId}`,
    )
    const rawData = await this.fetchSourceData(context)
    console.log(rawData)
    const parsedData = await this.parse(rawData, context)

    return {
      connector: this.name,
      serviceAccount: context.serviceAccount,
      pointId: context.pointId,
      generatedAt: new Date().toISOString(),
      data: parsedData,
    }
  }

  protected abstract fetchSourceData(
    context: ConnectorRunContext,
  ): Promise<unknown>

  protected abstract parse(
    rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload>
}
