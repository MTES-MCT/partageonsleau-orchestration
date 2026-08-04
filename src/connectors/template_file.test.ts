import assert from 'node:assert/strict'
import test from 'node:test'
import {normalizeTemplateDateOnly} from './template_file.js'

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000

function withTimezone(timezone: string, callback: () => void): void {
  const previousTimezone = process.env.TZ
  process.env.TZ = timezone

  try {
    callback()
  } finally {
    process.env.TZ = previousTimezone
  }
}

void test('normalise une date Excel convertie avec le decalage horaire d hiver', () => {
  const normalized = normalizeTemplateDateOnly(
    new Date('2024-11-30T22:59:39.000Z'),
  )

  assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
})

void test('normalise une date Excel convertie avec le decalage horaire d ete', () => {
  const normalized = normalizeTemplateDateOnly(
    new Date('2025-08-31T21:59:39.000Z'),
  )

  assert.equal(normalized?.toISOString(), '2025-09-01T00:00:00.000Z')
})

void test('preserve une date textuelle deja positionnee a minuit UTC', () => {
  const normalized = normalizeTemplateDateOnly('2025-06-01')

  assert.equal(normalized?.toISOString(), '2025-06-01T00:00:00.000Z')
})

void test('produit une duree en jours entiers entre deux dates du template', () => {
  const periodStart = normalizeTemplateDateOnly(
    new Date('2025-03-25T22:59:39.000Z'),
  )
  const periodEnd = normalizeTemplateDateOnly(
    new Date('2025-05-11T21:59:39.000Z'),
  )

  assert.ok(periodStart)
  assert.ok(periodEnd)
  assert.equal(
    (periodEnd.getTime() - periodStart.getTime()) / MILLISECONDS_PER_DAY,
    47,
  )
})

void test('neutralise aussi les fuseaux civils extremes appliques par xlsx', () => {
  withTimezone('Pacific/Kiritimati', () => {
    const normalized = normalizeTemplateDateOnly(
      new Date('2024-11-30T10:00:20.000Z'),
    )
    assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
  })

  withTimezone('Etc/GMT+12', () => {
    const normalized = normalizeTemplateDateOnly(
      new Date('2024-12-01T12:00:00.000Z'),
    )
    assert.equal(normalized?.toISOString(), '2024-12-01T00:00:00.000Z')
  })
})
