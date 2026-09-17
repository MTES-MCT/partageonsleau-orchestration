import assert from 'node:assert/strict'
import process from 'node:process'
import type {CellObject} from 'xlsx'
import {BvTechConnector} from '../bv-tech.js'
import {MetricType, PointFlowType} from '../types.js'
import xlsx from '../xlsx.js'

// Run by spreadsheet-compatibility.test.ts in a process with an explicit TZ.
const [sourceFile, dateSystem] = process.argv.slice(2)
assert.ok(sourceFile)
assert.ok(['1900', '1904', 'legacy'].includes(dateSystem))

const workbook = xlsx.readFile(sourceFile, {cellDates: true})
const sheet = workbook.Sheets['Mesures été']
assert.ok(sheet)

if (dateSystem === 'legacy') {
  assert.equal((sheet.A7 as CellObject).t, 'n')
  assert.equal((sheet.A8 as CellObject).t, 's')
} else {
  assert.equal(workbook.Workbook?.WBProps?.date1904, dateSystem === '1904')
  for (const [address, expectedDate] of [
    ['A7', '2026-01-02T00:00:00.000Z'],
    ['A8', '2026-06-02T00:00:00.000Z'],
    ['A9', '2026-06-03T12:30:00.000Z'],
  ]) {
    const cell = sheet[address] as CellObject
    assert.equal(cell.t, 'd', `${address} must exercise cellDates:true`)
    assert.ok(cell.v instanceof Date)
    assert.equal(cell.v.toISOString(), expectedDate)
  }
}

const output = await new BvTechConnector().run({
  sourceFile,
  sourcePointId: 'FORAGE-É',
  serviceAccount: 'synthetic-test',
  flowType: PointFlowType.PRELEVEMENT,
  rate: 100,
  mostRecentAvailableDate: new Date('1900-01-01T00:00:00.000Z'),
})
const volumes = output.data.metrics.find(
  (metric) => metric.type === MetricType.VOLUME,
)
const debits = output.data.metrics.find(
  (metric) => metric.type === MetricType.DEBIT,
)

// Daily volumes belong to J-1; debit readings retain the displayed day/time.
assert.deepEqual(
  volumes?.values.map(({date, value}) => ({date: date.toISOString(), value})),
  dateSystem === 'legacy'
    ? [
        {date: '2026-01-01T00:00:00.000Z', value: 12.5},
        {date: '2026-01-02T00:00:00.000Z', value: 15},
      ]
    : [
        {date: '2026-01-01T00:00:00.000Z', value: 12.5},
        {date: '2026-06-01T00:00:00.000Z', value: 15},
        {date: '2026-06-02T00:00:00.000Z', value: 17.5},
      ],
)
assert.deepEqual(
  debits?.values.map(({date, value}) => ({date: date.toISOString(), value})),
  dateSystem === 'legacy'
    ? [
        {date: '2026-01-02T00:00:00.000Z', value: 10},
        {date: '2026-01-03T00:00:00.000Z', value: 20},
      ]
    : [
        {date: '2026-01-02T00:00:00.000Z', value: 10},
        {date: '2026-06-02T00:00:00.000Z', value: 20},
        {date: '2026-06-03T12:30:00.000Z', value: 30},
      ],
)
