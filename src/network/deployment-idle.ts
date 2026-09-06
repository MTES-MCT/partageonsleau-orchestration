import {Buffer} from 'node:buffer'

const queueNames = ['pull-updated-data', 'process-declaration']

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function assertOrchestrationQueuesIdle(value: unknown): void {
  if (!isRecord(value) || !Array.isArray(value.queues)) {
    throw new Error('Invalid orchestration queue inventory')
  }

  const names = new Set<string>()
  for (const queue of value.queues) {
    if (
      !isRecord(queue) ||
      typeof queue.name !== 'string' ||
      !queueNames.includes(queue.name) ||
      names.has(queue.name) ||
      !isRecord(queue.counts)
    ) {
      throw new Error('Unexpected orchestration queue inventory')
    }

    names.add(queue.name)
    for (const state of ['active', 'waiting', 'prioritized']) {
      if (queue.counts[state] !== 0) {
        throw new Error('Orchestration queues must be idle before deployment')
      }
    }
  }

  if (names.size !== queueNames.length) {
    throw new Error('Incomplete orchestration queue inventory')
  }
}

export async function assertRemoteOrchestrationIdle(options: {
  endpoint: string
  expectedHostname: string
  password: string
  fetcher?: typeof fetch
}): Promise<void> {
  try {
    const url = new URL(
      options.endpoint.includes('://')
        ? options.endpoint
        : `https://${options.endpoint}`,
    )
    if (
      url.protocol !== 'https:' ||
      url.hostname !== options.expectedHostname ||
      url.port ||
      url.username ||
      url.password ||
      url.pathname !== '/' ||
      url.search ||
      url.hash ||
      !options.password
    ) {
      throw new Error('Invalid orchestration endpoint')
    }

    url.pathname = '/admin/queues/api/queues'
    const response = await (options.fetcher ?? fetch)(url, {
      method: 'GET',
      headers: {
        Authorization: `Basic ${Buffer.from(`operator:${options.password}`).toString('base64')}`,
      },
      redirect: 'error',
      signal: AbortSignal.timeout(15_000),
    })
    if (response.status !== 200) {
      await response.body?.cancel()
      throw new Error('Orchestration queue inventory unavailable')
    }

    assertOrchestrationQueuesIdle(await response.json())
  } catch {
    // Do not surface provider bodies, credentials or fetch error causes in CI.
    throw new Error('Orchestration idle preflight failed; deployment refused')
  }
}
