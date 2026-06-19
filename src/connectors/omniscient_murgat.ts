import {BaseConnector} from './base-connector.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
  type ConnectorRunContext,
  type ParsedPointPayload,
  type TimeserieValue,
} from './types.js'

type OmniscientMonthResponse = {
  monthStartDate: string
  payload: unknown
}

type OmniscientFetchResult = {
  startDate: Date
  endDate: Date
  months: OmniscientMonthResponse[]
}

type OmniscientParsedResult = {
  values: TimeserieValue[]
  requestedMonthCount: number
  droppedValueCount: number
}

type DateTimeParts = {
  year: number
  month: number
  day: number
  hour: number
  minute: number
  second: number
}

const oneHourInMilliseconds = 60 * 60 * 1000
const litersPerCubicMeter = 1000
const omniscientTimeZone = 'Europe/Paris'

const frenchLocalDateTimeFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: omniscientTimeZone,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
})

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function pad2(value: number): string {
  return String(value).padStart(2, '0')
}

function getFormattedDateTimeParts(date: Date): DateTimeParts {
  const values: Partial<Record<keyof DateTimeParts, number>> = {}

  for (const part of frenchLocalDateTimeFormatter.formatToParts(date)) {
    if (
      part.type === 'year' ||
      part.type === 'month' ||
      part.type === 'day' ||
      part.type === 'hour' ||
      part.type === 'minute' ||
      part.type === 'second'
    ) {
      values[part.type] = Number(part.value)
    }
  }

  if (
    values.year === undefined ||
    values.month === undefined ||
    values.day === undefined ||
    values.hour === undefined ||
    values.minute === undefined ||
    values.second === undefined
  ) {
    throw new Error('[omniscient_murgat] Unable to format Europe/Paris date.')
  }

  return {
    year: values.year,
    month: values.month,
    day: values.day,
    hour: values.hour,
    minute: values.minute,
    second: values.second,
  }
}

function getTimeZoneOffsetMilliseconds(date: Date): number {
  const parts = getFormattedDateTimeParts(date)

  return (
    Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
    ) - date.getTime()
  )
}

function parseOmniscientLocalDateTime(value: string): Date | undefined {
  const match =
    /^(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2}) (?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})$/v.exec(
      value,
    )

  if (!match?.groups) {
    return undefined
  }

  const parts = {
    year: Number(match.groups.year),
    month: Number(match.groups.month),
    day: Number(match.groups.day),
    hour: Number(match.groups.hour),
    minute: Number(match.groups.minute),
    second: Number(match.groups.second),
  }

  if (Object.values(parts).some((part) => !Number.isInteger(part))) {
    return undefined
  }

  const utcGuess = new Date(
    Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
    ),
  )
  const offset = getTimeZoneOffsetMilliseconds(utcGuess)

  return new Date(utcGuess.getTime() - offset)
}

function listMonthStartDateStrings(startDate: Date, endDate: Date): string[] {
  const startParts = getFormattedDateTimeParts(startDate)
  const endParts = getFormattedDateTimeParts(endDate)
  const {year: startYear, month: startMonth} = startParts
  const monthStartDateStrings: string[] = []

  for (
    let year = startYear, month = startMonth;
    year < endParts.year || (year === endParts.year && month <= endParts.month);
    month++
  ) {
    if (month > 12) {
      year++
      month = 1
    }

    monthStartDateStrings.push(`${year}-${pad2(month)}-01`)
  }

  return monthStartDateStrings
}

function parseNumericValue(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }

  if (typeof value !== 'string') {
    return undefined
  }

  const parsed = Number(value.trim().replace(',', '.'))

  return Number.isFinite(parsed) ? parsed : undefined
}

function unwrapChartPayload(payload: unknown): Record<string, unknown> {
  let currentPayload = payload

  if (typeof currentPayload === 'string') {
    currentPayload = JSON.parse(currentPayload) as unknown
  }

  if (Array.isArray(currentPayload)) {
    return {}
  }

  if (!isRecord(currentPayload)) {
    throw new Error('[omniscient_murgat] Invalid chart response format.')
  }

  return currentPayload
}

function maxDate(left: Date, right: Date): Date {
  return left.getTime() > right.getTime() ? left : right
}

export class OmniscientMurgatConnector extends BaseConnector<
  OmniscientFetchResult,
  OmniscientParsedResult
> {
  private static readonly endpointOrigin = 'https://o-mniscient.org'
  private static readonly connectorEnabledDate =
    parseOmniscientLocalDateTime('2021-10-12 10:00:00') ??
    new Date('2021-10-12T08:00:00.000Z')

  private static readonly metric = {
    type: MetricType.VOLUME_PRELEVE,
    granularity: Granularity.HOUR,
    conflictPolicy: ConflictPolicy.SKIP_NEW_CHUNK,
    unit: MetricUnit.M3,
  } as const

  constructor() {
    super('omniscient_murgat')
  }

  protected async fetch(
    context: ConnectorRunContext,
  ): Promise<OmniscientFetchResult> {
    const resolvedStartDate = this.resolveStartDate({
      mostRecentAvailableDate: context.mostRecentAvailableDate,
      connectorEnabledDate: OmniscientMurgatConnector.connectorEnabledDate,
    })
    const startDate = maxDate(
      resolvedStartDate,
      OmniscientMurgatConnector.connectorEnabledDate,
    )
    const endDate = new Date()
    const monthStartDateStrings = listMonthStartDateStrings(startDate, endDate)
    const months: OmniscientMonthResponse[] = []

    for (const monthStartDateString of monthStartDateStrings) {
      const url =
        `${OmniscientMurgatConnector.endpointOrigin}/api/compteur/chart/` +
        `${monthStartDateString}/${encodeURIComponent(context.sourcePointId)}`

      const response = await fetch(url, {
        headers: {
          Accept: 'application/json',
        },
      })

      if (!response.ok) {
        const body = await response.text()
        throw new Error(
          `[${this.name}] O-mniscient request failed with status ${response.status}: ${body}`,
        )
      }

      months.push({
        monthStartDate: monthStartDateString,
        payload: (await response.json()) as unknown,
      })
    }

    return {
      startDate,
      endDate,
      months,
    }
  }

  protected async parse(
    rawData: OmniscientFetchResult,
    context: ConnectorRunContext,
  ): Promise<OmniscientParsedResult> {
    const values: TimeserieValue[] = []
    let droppedValueCount = 0

    for (const month of rawData.months) {
      const chartPayload = unwrapChartPayload(month.payload)

      for (const [rawDate, rawValue] of Object.entries(chartPayload)) {
        const date = parseOmniscientLocalDateTime(rawDate)
        const liters = parseNumericValue(rawValue)

        if (!date || liters === undefined) {
          droppedValueCount++
          continue
        }

        const periodEnd = new Date(date.getTime() + oneHourInMilliseconds)
        if (
          periodEnd.getTime() <= rawData.startDate.getTime() ||
          date.getTime() > rawData.endDate.getTime()
        ) {
          continue
        }

        values.push({
          date,
          value: liters / litersPerCubicMeter,
        })
      }
    }

    values.sort((left, right) => left.date.getTime() - right.date.getTime())

    if (droppedValueCount > 0) {
      console.warn(
        `[${this.name}] Dropped ${droppedValueCount} invalid values for compteur "${context.sourcePointId}".`,
      )
    }

    return {
      values,
      requestedMonthCount: rawData.months.length,
      droppedValueCount,
    }
  }

  protected async process(
    parsedData: OmniscientParsedResult,
    context: ConnectorRunContext,
  ): Promise<ParsedPointPayload> {
    const {minDate, maxDate} = this.getMinMaxDates(
      parsedData.values,
      (value) => value.date,
    )

    return {
      id_point_de_prelevement: context.sourcePointId,
      source_type: SourceType.API,
      source_metadata: {
        provider: 'o-mniscient',
        site: 'pisciculture-murgat',
        endpoint: `${OmniscientMurgatConnector.endpointOrigin}/api/compteur/chart/{date}/{compteur}`,
        compteur: context.sourcePointId,
        requested_month_count: parsedData.requestedMonthCount,
        dropped_value_count: parsedData.droppedValueCount,
        source_unit: 'litre',
        unit_conversion: 'litre_to_m3',
        timezone: omniscientTimeZone,
      },
      min_date: minDate,
      max_date: maxDate,
      metrics: [
        {
          type: OmniscientMurgatConnector.metric.type,
          granularity: OmniscientMurgatConnector.metric.granularity,
          conflictPolicy: OmniscientMurgatConnector.metric.conflictPolicy,
          values: parsedData.values,
          unit: OmniscientMurgatConnector.metric.unit,
        },
      ],
    }
  }
}
