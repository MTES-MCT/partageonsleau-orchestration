import {WillieConnector} from './willie.js'
import {OrangeLiveObjectsConnector} from './orange_live_objects.js'
import {AquasysConnector} from './aquasys.js'
import {TemplateFileConnector} from './template_file.js'
import {type BaseConnector} from './base-connector.js'

export const connectorRegistry = new Map<
  string,
  BaseConnector<unknown, unknown>
>([
  ['willie', new WillieConnector()],
  ['orange_live_objects', new OrangeLiveObjectsConnector()],
  ['aquasys', new AquasysConnector()],
  ['template_file', new TemplateFileConnector()],
])
