import assert from 'node:assert/strict'
import test from 'node:test'
import {isSchedulerEnabled, selectedWorkerNames} from './execution-options.js'

void test('local rehearsal runs declaration workers only and leaves scheduling disabled', () => {
  const names = [
    'process-declaration',
    'pull-updated-data',
    'pull-rives-et-eaux',
  ]
  const environment = {
    WORKER_JOBS: 'process-declaration',
    SCHEDULER_ENABLED: 'false',
  }
  assert.deepEqual(selectedWorkerNames(names, environment), [
    'process-declaration',
  ])
  assert.equal(isSchedulerEnabled(environment), false)
  assert.deepEqual(selectedWorkerNames(names, {}), names)
  assert.equal(isSchedulerEnabled({}), true)
  assert.throws(
    () => selectedWorkerNames(names, {WORKER_JOBS: 'typo'}),
    /Unknown WORKER_JOBS/,
  )
})
