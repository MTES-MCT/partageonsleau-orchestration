import assert from 'node:assert/strict'
import test from 'node:test'
import {
  buildPointsForDetectedSourceIds,
  resolveConnectorName,
  resolveSourcePointId,
  type DeclarationPoint,
} from './process-declaration.js'

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
