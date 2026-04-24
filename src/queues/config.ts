import {Queue} from 'bullmq'
import {getRedisConnection} from './redis.js'

export const JOBS = [
  {
    name: 'pull-updated-data',
    cron: '0 0 3 * * *',
  },
] as const

export type JobName = (typeof JOBS)[number]['name']

const queues = new Map<JobName, Queue>()

export function getConnection() {
  return getRedisConnection()
}

const queueOptions = {
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: false,
    attempts: 3,
    backoff: {
      type: 'exponential' as const,
      delay: 5000,
    },
  },
}

export function getQueue(name: JobName) {
  if (queues.has(name)) {
    return queues.get(name)!
  }

  const connection = getConnection()

  const queue = new Queue(name, {
    connection,
    ...queueOptions,
  })

  queues.set(name, queue)

  return queue
}

export async function closeQueues() {
  await Promise.all([...queues.values()].map(async (queue) => queue.close()))

  queues.clear()
}
