import {Buffer} from 'node:buffer'
import express, {type Router} from 'express'
import {createBullBoard} from '@bull-board/api'
import {BullMQAdapter} from '@bull-board/api/bullMQAdapter'
import {ExpressAdapter} from '@bull-board/express'
import {type Queue} from 'bullmq'
import {getQueue, JOBS} from './config.js'

type BullBoardOptions = {
  queues?: Queue[]
}

type BullBoardRouter = {
  router: Router
  close: () => Promise<void>
}

export function createBullBoardRouter(
  basePath: string,
  password: string,
  options: BullBoardOptions = {},
): BullBoardRouter {
  const serverAdapter = new ExpressAdapter()
  serverAdapter.setBasePath(basePath)

  const rawQueues = options.queues ?? JOBS.map((job) => getQueue(job.name))
  const queues = rawQueues.map((queue) => new BullMQAdapter(queue))

  createBullBoard({
    queues,
    serverAdapter,
  })

  // eslint-disable-next-line new-cap
  const router = express.Router()

  router.use((request, response, next) => {
    const authHeader = request.headers.authorization

    if (!authHeader?.startsWith('Basic ')) {
      response.setHeader('WWW-Authenticate', 'Basic realm="BullBoard"')
      return response.status(401).json({error: 'Authentification requise'})
    }

    const base64Credentials = authHeader.split(' ')[1]
    const credentials = Buffer.from(base64Credentials, 'base64').toString(
      'ascii',
    )
    const [, pwd] = credentials.split(':')

    if (pwd !== password) {
      response.setHeader('WWW-Authenticate', 'Basic realm="BullBoard"')
      return response.status(401).json({error: 'Authentification échouée'})
    }

    next()
  })

  router.use(serverAdapter.getRouter())

  async function close() {
    await Promise.all(rawQueues.map(async (queue) => queue?.close?.()))
  }

  return {router, close}
}
