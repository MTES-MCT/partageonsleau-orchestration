import moment from 'moment'
import XLSX from './xlsx.js'

export function normalizeReunionText(value: string): string {
  return value
    .normalize('NFD')
    .replaceAll(/[\u0300-\u036f]/g, '')
    .replaceAll('’', "'")
    .trim()
    .toLowerCase()
    .replaceAll(/\s+/g, ' ')
}

/** Decimal text never passes through Number, including exponent notation. */
export function reunionDecimal(value: unknown): string | undefined {
  if (typeof value !== 'string' && typeof value !== 'number') return undefined
  if (typeof value === 'number' && !Number.isFinite(value)) return undefined
  const input = String(value)
    .trim()
    .replaceAll(/[\s\u00a0\u202f]/g, '')
    .replace(',', '.')
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:e([+-]?\d+))?$/i.exec(input)
  if (!match || !(match[2] || match[3]) || input.length > 512) return undefined
  const exponent = Number(match[4] ?? '0')
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 300)
    return undefined
  let digits = (match[2] || '0') + (match[3] ?? '')
  let scale = (match[3]?.length ?? 0) - exponent
  if (scale < 0) {
    digits += '0'.repeat(-scale)
    scale = 0
  }
  digits = digits.padStart(scale + 1, '0')
  let result = scale
    ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}`
    : digits
  result = result
    .replace(/^0+(?=\d)/, '')
    .replace(/(\.\d*?)0+$/, '$1')
    .replace(/\.$/, '')
  return result === '0' ? '0' : `${match[1] === '-' ? '-' : ''}${result}`
}

export function reunionDate(
  value: unknown,
  date1904 = false,
): string | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    const ssf = XLSX.SSF as {
      parse_date_code: (
        serial: number,
        options: {date1904: boolean},
      ) => {y: number; m: number; d: number} | null
    }
    const parsed = ssf.parse_date_code(value, {date1904})
    if (!parsed) return undefined
    return reunionDate(
      `${parsed.y}-${String(parsed.m).padStart(2, '0')}-${String(parsed.d).padStart(2, '0')}`,
    )
  }
  if (value instanceof Date && Number.isFinite(value.getTime()))
    return value.toISOString().slice(0, 10)
  if (typeof value !== 'string') return undefined
  let text = value.trim().replace(/['’]+$/, '')
  const months = [
    'janvier',
    'fevrier',
    'mars',
    'avril',
    'mai',
    'juin',
    'juillet',
    'aout',
    'septembre',
    'octobre',
    'novembre',
    'decembre',
  ]
  const french = /^(?:[a-z]+\s+)?(\d{1,2})\s+([a-z]+)\s+(\d{4})$/i.exec(
    normalizeReunionText(text),
  )
  if (french && months.includes(french[2]))
    text = `${french[3]}-${String(months.indexOf(french[2]) + 1).padStart(2, '0')}-${french[1].padStart(2, '0')}`
  const parsed = moment.utc(
    text,
    [
      'YYYY-MM-DD',
      'D/M/YYYY',
      'DD/MM/YYYY',
      'D-M-YYYY',
      'YYYY/MM/DD',
      'D/M/YYYY HH:mm:ss',
    ],
    true,
  )
  return parsed.isValid() ? parsed.format('YYYY-MM-DD') : undefined
}

export function reunionTime(value: unknown): string | undefined {
  if (
    typeof value === 'number' &&
    Number.isFinite(value) &&
    value >= 0 &&
    value < 2958466
  ) {
    const millis = Math.round((value % 1) * 86400000)
    if (millis >= 86400000) return undefined
    return new Date(millis).toISOString().slice(11, 23)
  }
  if (typeof value !== 'string') return undefined
  const match =
    /^(\d{1,2})(?::|h)(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,3}))?)?$/.exec(
      value.trim(),
    )
  if (
    !match ||
    Number(match[1]) > 23 ||
    Number(match[2]) > 59 ||
    Number(match[3] ?? 0) > 59
  )
    return undefined
  return `${match[1].padStart(2, '0')}:${match[2].padStart(2, '0')}:${(match[3] ?? '0').padStart(2, '0')}.${(match[4] ?? '').padEnd(3, '0')}`
}

export function reunionFrequency(value: string): string | undefined {
  const text = normalizeReunionText(value).replaceAll(/\s+/g, '')
  const aliases: Record<string, string[]> = {
    instant: ['instant', 'instantanee', 'instantaneous'],
    '1 second': ['seconde', '1seconde', 'second', '1second'],
    '1 minute': ['minute', '1minute'],
    '15 minutes': ['15minutes', '15min', '15mn', '15m', '15_minutes'],
    '1 hour': ['heure', '1heure', 'hour', '1hour'],
    '6 hours': ['6heures', '6hours'],
    '1 day': ['jour', '1jour', 'day', '1day'],
    '1 week': ['semaine', '1semaine', 'week', '1week'],
    '1 month': ['mois', '1mois', 'month', '1month'],
    '1 quarter': ['trimestre', '1trimestre', 'quarter', '1quarter'],
    '1 year': ['annee', '1annee', 'an', '1an', 'year', '1year'],
  }
  return Object.entries(aliases).find(([, forms]) => forms.includes(text))?.[0]
}

/** Reunion is UTC+04 all year; do not apply the host or metropolitan timezone. */
export function reunionPeriod(
  day: string,
  time: string,
  frequency: string,
  punctual: boolean,
): {date: string; periodStart: string; periodEnd: string} {
  const start = moment.parseZone(`${day}T${time}+04:00`, moment.ISO_8601, true)
  if (!start.isValid()) throw new Error('Date ou heure invalide.')
  const end = start.clone()
  if (!punctual) {
    const durations: Record<
      string,
      [number, moment.unitOfTime.DurationConstructor]
    > = {
      '1 second': [1, 'second'],
      '1 minute': [1, 'minute'],
      '15 minutes': [15, 'minutes'],
      '1 hour': [1, 'hour'],
      '6 hours': [6, 'hours'],
      '1 day': [1, 'day'],
      '1 week': [1, 'week'],
      '1 month': [1, 'month'],
      '1 quarter': [3, 'months'],
      '1 year': [1, 'year'],
    }
    const duration = durations[frequency]
    if (!duration)
      throw new Error('Une période cumulative exige une fréquence connue.')
    end.add(duration[0], duration[1])
  }
  return {
    date: start.toISOString(),
    periodStart: start.toISOString(),
    periodEnd: end.toISOString(),
  }
}

/** Camion templates may state an inclusive range instead of a daily date. */
export function reunionCamionPeriod(
  value: unknown,
  date1904 = false,
):
  | {
      date: string
      periodStart: string
      periodEnd: string
      frequency: '1 day' | 'irregular'
    }
  | undefined {
  const range =
    typeof value === 'string'
      ? /^\s*(\d{1,2}\/\d{1,2}\/\d{4})\s*au\s*(\d{1,2}\/\d{1,2}\/\d{4})\s*$/i.exec(
          value,
        )
      : null
  if (range) {
    const firstDay = reunionDate(range[1])
    const lastDay = reunionDate(range[2])
    if (!firstDay || !lastDay || firstDay > lastDay) return undefined
    const first = reunionPeriod(firstDay, '00:00:00.000', '1 day', false)
    const last = reunionPeriod(lastDay, '00:00:00.000', '1 day', false)
    return {...first, periodEnd: last.periodEnd, frequency: 'irregular'}
  }
  const day = reunionDate(value, date1904)
  return day
    ? {
        ...reunionPeriod(day, '00:00:00.000', '1 day', false),
        frequency: '1 day',
      }
    : undefined
}
