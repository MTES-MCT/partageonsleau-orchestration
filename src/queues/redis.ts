import process from 'node:process'
import fs from 'node:fs'
import path from 'node:path'
import {Redis, type RedisOptions} from 'ioredis'
import * as Sentry from '@sentry/node'

const isTest = process.env.NODE_ENV === 'test'

let redisConnection: Redis | null | undefined = null

export function getRedisConnection(): Redis {
  if (redisConnection) {
    return redisConnection
  }

  const url = process.env.REDIS_URL ?? 'redis://localhost:6380'
  const redisTlsCaFilePath = process.env.REDIS_TLS_CA_FILE_PATH

  const options: RedisOptions = {
    retryStrategy: (times: number) => Math.min(15_000, 250 * 2 ** times),
    maxRetriesPerRequest: null,
    lazyConnect: true,
  }

  if (redisTlsCaFilePath) {
    options.tls = {
      ca: fs.readFileSync(
        path.resolve(process.cwd(), redisTlsCaFilePath),
        'utf8',
      ),
    }
  }

  redisConnection = new Redis(url, options)

  if (!isTest) {
    redisConnection.on('ready', () => {
      console.log('✓ Redis ready')
    })
    redisConnection.on('reconnecting', () => {
      console.log('↻ Redis reconnecting...')
    })
    redisConnection.on('error', (error: unknown) => {
      console.warn('✗ Redis error:', error)
      Sentry.captureException(error)
    })
  }

  return redisConnection
}

export async function waitForRedisConnection() {
  if (isTest) {
    return
  }

  const redis = getRedisConnection()

  try {
    await redis.connect()
  } catch {}

  if (redis.status === 'ready') {
    return
  }

  await new Promise<void>((resolve, reject) => {
    const onReady = () => {
      cleanup()
      resolve()
    }

    const onError = (error: Error) => {
      cleanup()
      reject(error)
    }

    const cleanup = () => {
      redis.off('ready', onReady)
      redis.off('error', onError)
    }

    redis.once('ready', onReady)
    redis.once('error', onError)
  })
}

export async function closeRedisConnection() {
  if (!redisConnection) {
    return
  }

  const client = redisConnection
  redisConnection = null

  try {
    if (client.status === 'end') {
      return
    }

    if (client.status === 'wait') {
      client.disconnect(false)
      return
    }

    await client.quit()
  } catch {
    client.disconnect(false)
  }
}
