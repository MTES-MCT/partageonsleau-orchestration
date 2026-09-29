import assert from 'node:assert/strict'
import test from 'node:test'
import {
  buildPointsForDetectedSourceIds,
  metricToLegacySeries,
  resolveConnectorName,
  resolveSourcePointId,
  type DeclarationPoint,
} from './process-declaration.js'
import {
  ConflictPolicy,
  Granularity,
  MetricType,
  MetricUnit,
  SourceType,
} from '../connectors/types.js'

void test('le type smnpr utilise le connecteur SMNPR', () => {
  assert.equal(resolveConnectorName('smnpr'), 'smnpr')
})

void test('un BSS SMNPR retrouve le point et conserve son nom interne', () => {
  const declarationPoint: DeclarationPoint = {
    pointId: 'point-1',
    name: 'Forage communal',
    codeBSS: 'BSS002MUNP',
  }
  const [matchedPoint] = buildPointsForDetectedSourceIds({
    connectorName: 'smnpr',
    declarationPoints: [declarationPoint],
    detectedSourcePointIds: ['bss002munp'],
  })

  assert.equal(matchedPoint?.pointId, 'point-1')
  assert.equal(matchedPoint?.name, 'Forage communal')
  assert.equal(matchedPoint?.sourcePointId, 'bss002munp')
  assert.equal(
    resolveSourcePointId({connectorName: 'smnpr', point: matchedPoint}),
    'BSS002MUNP',
  )
})

void test('un BSS inconnu reste disponible pour le rapprochement', () => {
  const [detectedPoint] = buildPointsForDetectedSourceIds({
    connectorName: 'smnpr',
    declarationPoints: [],
    detectedSourcePointIds: ['BSS002MVMB'],
  })

  assert.deepEqual(detectedPoint, {
    pointId: 'detected:BSS002MVMB',
    name: 'BSS002MVMB',
    sourcePointId: 'BSS002MVMB',
  })
})

void test('plusieurs exploitations du même PP ne choisissent jamais le premier comptage', () => {
  const points = buildPointsForDetectedSourceIds({
    connectorName: 'template_file',
    declarationPoints: ['001', '002'].map((countingCode) => ({
      pointId: 'point-1',
      name: 'Forage communal',
      exploitationId: `exploitation-${countingCode}`,
      countingCode,
    })),
    detectedSourcePointIds: ['Forage communal', ' forage communal '],
  })
  assert.equal(points.length, 1)
  assert.equal(points[0].pointId, 'point-1')
  assert.equal(points[0].exploitationId, undefined)
  assert.equal(points[0].countingCode, undefined)
})

void test('un identifiant partagé par deux PP reste non rapproché', () => {
  const [point] = buildPointsForDetectedSourceIds({
    connectorName: 'smnpr',
    declarationPoints: [1, 2].map((id) => ({
      pointId: `point-${id}`,
      name: `Forage ${id}`,
      codeBSS: 'BSS-PARTAGE',
    })),
    detectedSourcePointIds: ['BSS-PARTAGE'],
  })
  assert.equal(point.pointId, 'detected:BSS-PARTAGE')
  assert.equal(point.name, 'BSS-PARTAGE')
})

void test('la conversion des séries transmet le code sans déduire une exploitation du contexte', () => {
  const series = metricToLegacySeries({
    point: {
      pointId: 'point-1',
      name: 'Forage communal',
      exploitationId: 'ancienne-exploitation',
      countingCode: 'ancien-code',
    },
    metric: {
      type: MetricType.VOLUME,
      countingCode: '001',
      granularity: Granularity.DAY,
      conflictPolicy: ConflictPolicy.REPLACE_EXISTING,
      unit: MetricUnit.M3,
      values: [{date: new Date('2026-07-01'), value: 10}],
    },
    payload: {
      id_point_de_prelevement: 'Forage communal',
      source_type: SourceType.DECLARATION,
      source_metadata: undefined,
      min_date: undefined,
      max_date: undefined,
      metrics: [],
    },
  })
  assert.equal(series?.countingCode, '001')
  assert.equal(series?.exploitationId, undefined)
  assert.equal(series?.data[0].value, 10)
})
