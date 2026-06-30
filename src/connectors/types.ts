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
  sourceFiles?: ConnectorSourceFile[]
}

export type ConnectorDiscoveryContext = {
  sourceFile?: string
  sourceFiles?: ConnectorSourceFile[]
}

export type ConnectorSourceFile = {
  type: string
  filename: string
  path: string
}

export type TimeserieValue = {
  date: Date
  value: number
}

export enum MetricType {
  INDEX = 'index',
  VOLUME_PRELEVE = 'volume_preleve',
  VOLUME_REJETE = 'volume_rejete',
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
  QUARTER = '1 quarter',
  YEAR = '1 year',
}

export enum SourceType {
  DECLARATION = 'DECLARATION',
  BATCH = 'BATCH',
  API = 'API',
}

export type WaterUseCode = string

export enum ConflictPolicy {
  REPLACE_EXISTING = 'REPLACE_EXISTING',
  SKIP_NEW_CHUNK = 'SKIP_NEW_CHUNK',
}

export type Timeserie = {
  type: MetricType
  usage?: WaterUseCode
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
