import {WillieConnector} from './willie.js';
import {OrangeLiveObjectsConnector} from './orange_live_objects.js';
import {AquasysConnector} from './aquasys.js';
import {type BaseConnector} from './base-connector.js';

export const connectorRegistry = new Map<string, BaseConnector>([
	['willie', new WillieConnector()],
	['orange_live_objects', new OrangeLiveObjectsConnector()],
	['aquasys', new AquasysConnector()],
]);
