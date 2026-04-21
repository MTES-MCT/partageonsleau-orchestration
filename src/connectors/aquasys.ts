import {BaseConnector} from './base-connector.js'
import type {ConnectorRunContext, ParsedPointPayload} from './types.js'

export class AquasysConnector extends BaseConnector {
  constructor() {
    super('aquasys')
  }

  protected async fetchSourceData(
    context: ConnectorRunContext,
  ): Promise<unknown> {
    // TODO: récupération fichier/API Aquasys.
    return {
      source: 'aquasys',
      pointId: context.pointId,
      lastRunAt: context.lastRunAt,
      records: [],
    }
  }

  protected async parse(
    _rawData: unknown,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    return {
      id_point_de_prelevement: context.pointId,
      metrics: [],
    }
  }
}
