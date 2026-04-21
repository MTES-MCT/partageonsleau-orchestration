export type ConfigEntry = {
  service_account: string
}

export type ServiceAccountPointContext = {
  sourcePointId: string
  connector: string
  lastRunAt: string | undefined
  most_recent_available_date: string | undefined
  sourceFiles?: string[]
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
  lastRunAt: string | undefined
  most_recent_available_date?: string
  sourceFiles?: string[]
}

export type TimeserieValue = {
  date: string
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

export type Timeserie = {
  type: MetricType
  granularity: Granularity
  values: TimeserieValue[]
  unit: MetricUnit | undefined
}

export type ParsedPointPayload = {
  id_point_de_prelevement: string
  source_type: SourceType
  source_metadata: Record<string, unknown> | undefined
  min_date: string | undefined
  max_date: string | undefined
  metrics: Timeserie[]
}

export type ConnectorOutput = {
  connector: string
  serviceAccount: string
  sourcePointId: string
  generatedAt: string
  data: ParsedPointPayload
}
