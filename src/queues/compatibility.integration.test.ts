import assert from 'node:assert/strict'
import {randomUUID} from 'node:crypto'
import process from 'node:process'
import {setTimeout as delay} from 'node:timers/promises'
import test from 'node:test'
import {Queue, Worker} from 'bullmq'
import {Queue as LegacyQueue, Worker as LegacyWorker} from 'bullmq-v5'

function testConnection() {
  const url = new URL(process.env.REDIS_URL ?? '')
  assert.ok(
    ['localhost', '127.0.0.1', '[::1]', 'redis'].includes(url.hostname),
    'Use a disposable local Redis service',
  )
  assert.equal(url.protocol, 'redis:')
  assert.ok(
    ['/1', '/2'].includes(url.pathname),
    'Compatibility tests require isolated Redis database 1 or 2',
  )
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
    db: Number(url.pathname.slice(1)),
    maxRetriesPerRequest: null,
    protocol: 2 as const,
  }
}

async function waitFor(predicate: () => Promise<boolean>) {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    if (await predicate()) {
      return
    }

    await delay(25)
  }

  assert.fail('Queue did not reach the expected state within 10 seconds')
}

for (const [from, to] of [
  [5, 6],
  [6, 5],
] as const) {
  void test(
    `BullMQ ${from} → ${to} conserve les jobs, échecs, délais et schedulers`,
    {
      skip: process.env.QUEUE_INTEGRATION_TESTS !== '1',
      timeout: 30_000,
    },
    async (t) => {
      const connection = testConnection()
      const prefix = `ple-security-test-${randomUUID()}`
      const options = {connection, prefix}
      const queueName = 'process-declaration'
      const producer =
        from === 5
          ? new LegacyQueue(queueName, options)
          : new Queue(queueName, options)
      const consumer =
        to === 5
          ? new LegacyQueue(queueName, options)
          : new Queue(queueName, options)
      const workers: Array<{close: () => Promise<void>}> = []
      t.after(async () => {
        await Promise.all(workers.map(async (worker) => worker.close()))
        // This UUID prefix belongs only to this fixture, never an application queue.
        try {
          await consumer.obliterate({force: true})
        } finally {
          await Promise.all([producer.close(), consumer.close()])
        }
      })

      const failed = await producer.add(
        'failed-before-upgrade',
        {declarationId: 'synthetic-failed'},
        {jobId: 'job-failed', attempts: 1},
      )
      const fail = async () => {
        throw new Error('Synthetic failure before changing BullMQ version')
      }

      const oldWorker =
        from === 5
          ? new LegacyWorker(queueName, fail, options)
          : new Worker(queueName, fail, options)
      workers.push(oldWorker)
      await waitFor(async () => (await failed.getState()) === 'failed')
      await oldWorker.close()

      await producer.add(
        'waiting',
        {declarationId: 'synthetic-waiting'},
        {jobId: 'job-waiting'},
      )
      await producer.add(
        'waiting',
        {declarationId: 'must-not-replace'},
        {jobId: 'job-waiting'},
      )
      const waitingJob = await producer.getJob('job-waiting')
      assert.deepEqual(waitingJob?.data, {
        declarationId: 'synthetic-waiting',
      })
      await producer.add(
        'delayed',
        {declarationId: 'synthetic-delayed'},
        {jobId: 'job-delayed', delay: 250},
      )
      await producer.upsertJobScheduler(
        'pull-updated-data-daily',
        {every: 3_600_000},
        {
          name: 'scheduled',
          data: {trigger: 'synthetic-scheduler'},
        },
      )

      const failedAfterUpgrade = await consumer.getJob('job-failed')
      assert.ok(failedAfterUpgrade)
      assert.equal(await failedAfterUpgrade.getState(), 'failed')
      await failedAfterUpgrade.retry()
      const schedulers = await consumer.getJobSchedulers()
      assert.equal(schedulers.length, 1)
      const scheduler = await consumer.getJobScheduler(
        'pull-updated-data-daily',
      )
      assert.equal(scheduler?.name, 'scheduled')

      const processed = new Map<string, number>()
      const processJob = async (job: {name: string}) => {
        processed.set(job.name, (processed.get(job.name) ?? 0) + 1)
      }

      const worker =
        to === 5
          ? new LegacyWorker(queueName, processJob, options)
          : new Worker(queueName, processJob, options)
      workers.push(worker)
      await waitFor(async () => {
        const jobs = await Promise.all(
          ['job-failed', 'job-waiting', 'job-delayed'].map(async (id) =>
            consumer.getJob(id),
          ),
        )
        const states = await Promise.all(
          jobs.map(async (job) => job?.getState()),
        )
        return (
          states.every((state) => state === 'completed') &&
          processed.has('scheduled')
        )
      })
      await worker.close()
      assert.deepEqual(Object.fromEntries(processed), {
        waiting: 1,
        'failed-before-upgrade': 1,
        scheduled: 1,
        delayed: 1,
      })
      assert.equal(
        await consumer.removeJobScheduler('pull-updated-data-daily'),
        true,
      )
    },
  )
}
