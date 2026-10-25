/** Exact, opt-in contract. Existing provider metrics retain their numeric contract. */
export type NativeSeriesValue = {
  date: string
  periodStart: string
  periodEnd: string
  value: string
  remark?: string
  metadata: {row: number; cell: string; [key: string]: unknown}
}

export type NativeSeries = {
  nativeTemporal: true
  seriesKey: string
  pointPrelevement: string
  parameter: string
  unit: string
  frequency: string
  valueType: string
  autoCalculateVolumes: false
  minDate: string
  maxDate: string
  metadata: Record<string, unknown>
  data: NativeSeriesValue[]
}

export type ReunionParseError = {
  code: string
  severity: 'error' | 'warning'
  filename: string
  fileId?: string
  sheet?: string
  row?: number
  column?: number
  cell?: string
  seriesKey?: string
  message: string
  rawValue?: string
}

export type ReunionParseResult = {
  series: NativeSeries[]
  errors: ReunionParseError[]
}
