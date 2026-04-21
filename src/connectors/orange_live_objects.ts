import {BaseConnector} from './base-connector.js';
import type {ConnectorRunContext, ParsedPointPayload} from './types.js';

export class OrangeLiveObjectsConnector extends BaseConnector {
	constructor() {
		super('orange_live_objects');
	}

	protected async fetchSourceData(context: ConnectorRunContext): Promise<unknown> {
		// TODO: appel API Orange Live Objects.
		return {
			source: 'orange_live_objects',
			pointId: context.pointId,
			lastRunAt: context.lastRunAt,
			records: [],
		};
	}

	protected async parse(
		_rawData: unknown,
		context: ConnectorRunContext,
	): Promise<ParsedPointPayload> {
		return {
			id_point_de_prelevement: context.pointId,
			metrics: [],
		};
	}
}
