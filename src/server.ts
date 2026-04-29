import crypto from 'node:crypto'
import process from 'node:process'
import {Buffer} from 'node:buffer'
import {type IncomingMessage} from 'node:http'
import express, {type NextFunction, type Request, type Response} from 'express'
import {Sentry} from './instrument.js'
import {addJobProcessDeclaration, addJobPullUpdatedData} from './queues/jobs.js'
import {closeRedisConnection, waitForRedisConnection} from './queues/redis.js'
import {startScheduler} from './queues/scheduler.js'
import {startWorkers} from './queues/workers.js'
import {closeQueues} from './queues/config.js'

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

const rawBodyByRequest = new WeakMap<IncomingMessage | Request, string>()

const app = express()

app.use(
  express.json({
    verify(request, _response, buffer) {
      rawBodyByRequest.set(request, buffer.toString('utf8'))
    },
  }),
)

function verifyPleSignature(request: Request): void {
  const secret = process.env.PLE_WEBHOOK_SECRET

  if (!secret) {
    throw new Error('PLE_WEBHOOK_SECRET is not configured')
  }

  const signature = request.get('X-PLE-Signature')

  if (!signature) {
    throw new Error('Missing X-PLE-Signature header')
  }

  const rawBody = rawBodyByRequest.get(request)

  if (!rawBody) {
    throw new Error('Missing raw request body')
  }

  const expectedSignature = crypto
    .createHmac('sha256', secret)
    .update(rawBody)
    .digest('hex')

  const signatureBuffer = Buffer.from(signature, 'hex')
  const expectedSignatureBuffer = Buffer.from(expectedSignature, 'hex')

  if (signatureBuffer.length !== expectedSignatureBuffer.length) {
    throw new Error('Invalid webhook signature')
  }

  const isValid = crypto.timingSafeEqual(
    signatureBuffer,
    expectedSignatureBuffer,
  )

  if (!isValid) {
    throw new Error('Invalid webhook signature')
  }
}

app.get('/health', (_request, response) => {
  response.status(200).json({ok: true})
})

app.get('/debug-sentry', () => {
  throw new Error('Sentry test error')
})

app.post('/jobs/pull-updated-data', async (_request, response, next) => {
  try {
    const job = await addJobPullUpdatedData({trigger: 'http'})

    response.status(202).json({
      ok: true,
      jobId: job?.id ?? null,
    })
  } catch (error) {
    next(error)
  }
})

app.post(
  '/hooks/declarations',
  async (
    request: Request<Record<string, never>, unknown, unknown>,
    response,
    next,
  ) => {
    try {
      verifyPleSignature(request)

      const {body} = request

      if (!isObjectRecord(body)) {
        response.status(400).json({
          ok: false,
          error: 'Invalid request body',
        })
        return
      }

      const {event, declarationId} = body

      if (event !== 'declaration.uploaded') {
        response.status(400).json({
          ok: false,
          error: 'Invalid event',
        })
        return
      }

      if (typeof declarationId !== 'string' || !declarationId) {
        response.status(400).json({
          ok: false,
          error: 'Missing declarationId',
        })
        return
      }

      const job = await addJobProcessDeclaration({declarationId})

      response.status(202).json({
        ok: true,
        jobId: job?.id ?? null,
      })
    } catch (error) {
      next(error)
    }
  },
)

app.use(
  (
    error: unknown,
    _request: Request,
    response: Response,
    _next: NextFunction,
  ) => {
    Sentry.captureException(error)
    console.error(error)

    response.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : 'Internal server error',
    })
  },
)

const port = Number(process.env.PORT ?? 4000)

try {
  await waitForRedisConnection()
  await startScheduler()

  const workers = startWorkers()

  const server = app.listen(port, () => {
    console.log(`Server listening on port ${port}`)
  })

  let isShuttingDown = false

  const shutdown = async (signal: string) => {
    if (isShuttingDown) {
      return
    }

    isShuttingDown = true

    console.log(`Received ${signal}, shutting down...`)

    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error)
          return
        }

        resolve()
      })
    })

    await Promise.all(workers.map(async (worker) => worker.close()))
    await closeQueues()
    await closeRedisConnection()
    await Sentry.close(2000)
  }

  process.on('SIGINT', () => {
    void shutdown('SIGINT')
  })

  process.on('SIGTERM', () => {
    void shutdown('SIGTERM')
  })
} catch (error: unknown) {
  Sentry.captureException(error)
  console.error('Failed to start server', error)
  await Sentry.close(2000)
  throw error
}
