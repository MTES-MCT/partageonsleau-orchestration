import assert from 'node:assert/strict'
import {readFileSync} from 'node:fs'
import test from 'node:test'

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8')
const nodeVersion = read('.nvmrc').trim()
const manifest = JSON.parse(read('package.json'))
const lockfile = JSON.parse(read('package-lock.json'))

test('Node is pinned consistently in local tooling, package engines and the immutable image', () => {
  assert.match(nodeVersion, /^\d+\.\d+\.\d+$/)
  const major = Number(nodeVersion.split('.')[0])
  assert.equal(manifest.engines.node, `>=${nodeVersion} <${major + 1}`)
  assert.deepEqual(lockfile.packages[''].engines, manifest.engines)

  const dockerfile = read('deploy/Dockerfile')
  const base = dockerfile.match(/^FROM node:(\d+\.\d+\.\d+)-alpine\d+\.\d+@sha256:[a-f0-9]{64} AS base$/m)
  assert.ok(base, 'The Node base must be pinned by version and immutable digest')
  assert.equal(base[1], nodeVersion)
})

test('The verified image smoke test rejects a runtime different from .nvmrc', () => {
  const smoke = read('.github/scripts/smoke-image.sh')
  assert.ok(smoke.includes('--env "EXPECTED_NODE=$(<.nvmrc)"'))
  assert.ok(smoke.includes('process.versions.node !== process.env.EXPECTED_NODE'))
})
