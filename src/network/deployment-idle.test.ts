import assert from 'node:assert/strict'
import test from 'node:test'
import {
  assertOrchestrationQueuesIdle,
  assertRemoteOrchestrationIdle,
} from './deployment-idle.js'

function idleQueues() {
  return {
    queues: ['pull-updated-data', 'process-declaration'].map((name) => ({
      name,
      counts: {active: 0, waiting: 0, prioritized: 0, delayed: 1},
    })),
  }
}

void test('deployment preflight accepts idle orchestration queues with delayed schedules', () => {
  assertOrchestrationQueuesIdle(idleQueues())
})

void test('deployment preflight rejects active, waiting or prioritized work', () => {
  for (const state of ['active', 'waiting', 'prioritized'] as const) {
    const inventory = idleQueues()
    inventory.queues[0].counts[state] = 1
    assert.throws(() => {
      assertOrchestrationQueuesIdle(inventory)
    })
  }
})

void test('deployment preflight refuses missing, duplicate and unrelated queues', () => {
  const queue = idleQueues().queues[0]
  for (const value of [
    undefined,
    {},
    {queues: []},
    {queues: [queue]},
    {queues: [queue, queue]},
    {queues: [{...queue, name: 'unrelated'}, idleQueues().queues[1]]},
    {queues: [{...queue, counts: {}}, idleQueues().queues[1]]},
  ]) {
    assert.throws(() => {
      assertOrchestrationQueuesIdle(value)
    })
  }
})

void test('deployment preflight uses only the bounded authenticated GET without following redirects', async () => {
  await assertRemoteOrchestrationIdle({
    endpoint: 'orchestration.example.invalid',
    expectedHostname: 'orchestration.example.invalid',
    password: 'test-credential',
    async fetcher(input, init) {
      assert.ok(input instanceof URL)
      assert.equal(
        input.toString(),
        'https://orchestration.example.invalid/admin/queues/api/queues',
      )
      assert.equal(init?.method, 'GET')
      assert.equal(init?.redirect, 'error')
      assert.equal(
        new Headers(init?.headers).get('Authorization'),
        'Basic b3BlcmF0b3I6dGVzdC1jcmVkZW50aWFs',
      )
      assert.ok(init?.signal)
      return Response.json(idleQueues())
    },
  })
})

void test('deployment preflight refuses endpoints outside the exact configured HTTPS origin', async () => {
  for (const endpoint of [
    'http://orchestration.example.invalid',
    'https://other.example.invalid',
    'https://child.orchestration.example.invalid',
    'https://orchestration.example.invalid:8443',
    'https://user:password@orchestration.example.invalid',
    'https://orchestration.example.invalid/path',
    'https://orchestration.example.invalid?query=1',
    'https://orchestration.example.invalid#fragment',
  ]) {
    let fetched = false
    await assert.rejects(
      assertRemoteOrchestrationIdle({
        endpoint,
        expectedHostname: 'orchestration.example.invalid',
        password: 'test-credential',
        async fetcher() {
          fetched = true
          return Response.json(idleQueues())
        },
      }),
    )
    assert.equal(fetched, false)
  }
})

void test('deployment preflight fails closed without exposing credentials or response bodies', async () => {
  for (const fetcher of [
    async () => new Response('sensitive-response', {status: 401}),
    async () => new Response('sensitive-invalid-json'),
    async () => {
      throw new Error('test-credential')
    },
  ]) {
    await assert.rejects(
      assertRemoteOrchestrationIdle({
        endpoint: 'https://orchestration.example.invalid',
        expectedHostname: 'orchestration.example.invalid',
        password: 'test-credential',
        fetcher,
      }),
      (error: unknown) => {
        assert.ok(error instanceof Error)
        assert.equal(
          error.message,
          'Orchestration idle preflight failed; deployment refused',
        )
        assert.equal(error.cause, undefined)
        return true
      },
    )
  }
})
