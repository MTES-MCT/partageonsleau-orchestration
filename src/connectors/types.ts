export type ConfigEntry = {
  service_account: string
}

export type ServiceAccountPointContext = {
  pointId: string
  sourcePointId: string
  connector: string
  connectorId?: string
  connectorRate: number
  mostRecentAvailableDate: Date | undefined
  sourceFile?: string
}

export type ServiceAccountContext = {
  serviceAccount: string
  points: ServiceAccountPointContext[]
}

export type DeclarantContext = {
  contextId: string
  points: ServiceAccountPointContext[]
}

export type ConnectorRunContext = {
  serviceAccount: string
  sourcePointId: string
  connectorId?: string
  rate: number
  mostRecentAvailableDate: Date | undefined
  sourceFile?: string
}

export type TimeserieValue = {
  date: Date
  value: number
}

export enum MetricType {
  INDEX = 'index',
  VOLUME_PRELEVE = 'volume_preleve',
}

export enum MetricUnit {
  M3 = 'm3',
}

export enum Granularity {
  FIFTEEN_MINUTES = '15_minutes',
  HOUR = '1 hour',
  DAY = '1 day',
  WEEK = '1 week',
  MONTH = '1 month',
  YEAR = '1 year',
}

export enum SourceType {
  DECLARATION = 'DECLARATION',
  BATCH = 'BATCH',
  API = 'API',
}

export type UsageEau =
  | 'INCONNU'
  | 'PAS_D_USAGE'
  | 'IRRIGATION'
  | 'AGRICULTURE_ELEVAGE'
  | 'AQUACULTURE'
  | 'INDUSTRIE'
  | 'AEP'
  | 'ENERGIE'
  | 'LOISIRS'
  | 'EMBOUTEILLAGE'
  | 'THERMALISME_THALASSO'
  | 'DEFENSE_INCENDIE'
  | 'REALIMENTATION_EAU'
  | 'CANAUX'
  | 'ETIAGE'
  | 'ENTRETIEN_VOIRIES'
  | 'ALIMENTATION_SOUTIEN_CANAL'
  | 'DOMESTIQUE'

export enum ConflictPolicy {
  REPLACE_EXISTING = 'REPLACE_EXISTING',
  SKIP_NEW_CHUNK = 'SKIP_NEW_CHUNK',
}

export type Timeserie = {
  type: MetricType
  usage?: UsageEau
  granularity: Granularity
  conflictPolicy: ConflictPolicy
  values: TimeserieValue[]
  unit: MetricUnit | undefined
}

export type ParsedPointPayload = {
  id_point_de_prelevement: string
  source_type: SourceType
  source_metadata: Record<string, unknown> | undefined
  min_date: Date | undefined
  max_date: Date | undefined
  metrics: Timeserie[]
}

export type ConnectorOutput = {
  connector: string
  serviceAccount: string
  sourcePointId: string
  connectorId?: string
  connectorRate: number
  lastRunAt: Date
  data: ParsedPointPayload
}
