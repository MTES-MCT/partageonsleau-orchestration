export type ConfigEntry = {
  service_account: string
}

export type ServiceAccountPointContext = {
  pointId: string
  flowType?: PointFlowType
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
  flowType?: PointFlowType
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
  // Bornes semi-ouvertes [periodStart, periodEnd) quand la source décrit une plage.
  periodStart?: Date
  periodEnd?: Date
  value: number
}

export enum MetricType {
  INDEX = 'index',
  VOLUME = 'volume',
  DEBIT = 'debit',
}

export enum PointFlowType {
  PRELEVEMENT = 'PRELEVEMENT',
  REJET = 'REJET',
}

export enum MetricUnit {
  M3 = 'm3',
  L_S = 'L/s',
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
  SKIP_CONFLICTING_VALUES = 'SKIP_CONFLICTING_VALUES',
  REPLACE_EXISTING_EXCEPT_WILLIE = 'REPLACE_EXISTING_EXCEPT_WILLIE',
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
  flow_type?: PointFlowType
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
