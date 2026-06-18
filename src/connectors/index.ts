import {WillieConnector} from './willie.js'
import {OrangeLiveObjectsConnector} from './orange_live_objects.js'
import {AquasysConnector} from './aquasys.js'
import {TemplateFileConnector} from './template_file.js'
import {GidafConnector} from './gidaf.js'
import {type BaseConnector} from './base-connector.js'
import {BvTechConnector} from './bv-tech.js'

export const connectorRegistry = new Map<
  string,
  BaseConnector<unknown, unknown>
>([
  ['willie', new WillieConnector()],
  ['bv_tech', new BvTechConnector()],
  ['orange_live_objects', new OrangeLiveObjectsConnector()],
  ['aquasys', new AquasysConnector()],
  ['template_file', new TemplateFileConnector()],
  ['gidaf', new GidafConnector()],
])
