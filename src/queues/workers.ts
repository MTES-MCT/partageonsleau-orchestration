import {Worker, type Job} from 'bullmq'
import * as Sentry from '@sentry/node'
import {connectorRegistry} from '../connectors/index.js'
import {pullUpdatedData} from '../jobs/pull_updated_data.js'
import {processDeclaration} from '../jobs/process-declaration.js'
import {getConnection, JOBS} from './config.js'

const handlers: Record<string, (job: Job) => Promise<void>> = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  async 'pull-updated-data'(_job) {
    await pullUpdatedData(connectorRegistry)
  },

  // eslint-disable-next-line @typescript-eslint/naming-convention
  async 'process-declaration'(job) {
    const declarationId = job.data?.declarationId

    if (typeof declarationId !== 'string' || !declarationId) {
      throw new Error('[process-declaration] Missing declarationId')
    }

    await processDeclaration(declarationId)
  },
}

export function startWorkers() {
  const connection = getConnection()

  return JOBS.map(({name}) => {
    const handler = handlers[name]

    const worker = new Worker(name, handler, {
      connection,
      concurrency: 1,
    })

    worker.on('error', (error) => {
      Sentry.captureException(error)
    })

    worker.on('failed', (job, error) => {
      const message = error?.message ?? String(error)
      const stack = error?.stack ?? ''

      console.error(
        `[worker ${name}] Job ${job?.id} failed:`,
        message,
        stack ? `\n${stack}` : '',
      )

      Sentry.withScope((scope) => {
        scope.setTag('queue', name)
        scope.setContext('job', {
          id: job?.id,
          name: job?.name,
          data: job?.data,
        })
        Sentry.captureException(error)
      })
    })

    worker.on('completed', (job) => {
      console.log(`[worker ${name}] Job ${job.id} completed`)
    })

    return worker
  })
}
