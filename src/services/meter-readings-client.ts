export type MeterStreamContext = {
  provider: string
  scope: string
  streams: Array<{
    id: string
    externalId: string
    compteurId: string
    activatedAt: string
  }>
}

export type MeterReading = {
  externalId: string | null
  observedAt: string | null
  index: string | null
  status: 'VALID' | 'INVALID'
  reason?: string | null
  quality?: string | null
  origin?: string | null
  raw?: unknown
}

export type MeterIngestionBatch = {
  provider: string
  scope: string
  batchId: string
  windowStart: string
  windowEnd: string
  fetchedAt: string
  complete: true
  mode: 'LIVE'
  readings: MeterReading[]
}

export type MeterIngestionResult = {
  persisted: true
  ingestionId: string
  checkpoint: string
  counts: Record<string, number>
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isMeterStreamContext(
  context: unknown,
  provider: string,
  scope: string,
): context is MeterStreamContext {
  return (
    isRecord(context) &&
    context.provider === provider &&
    context.scope === scope &&
    Array.isArray(context.streams) &&
    context.streams.every(
      (stream: unknown) =>
        isRecord(stream) &&
        typeof stream.id === 'string' &&
        typeof stream.externalId === 'string' &&
        typeof stream.compteurId === 'string' &&
        typeof stream.activatedAt === 'string' &&
        Number.isFinite(Date.parse(stream.activatedAt)),
    )
  )
}

function isMeterIngestionResult(
  result: unknown,
): result is MeterIngestionResult {
  const requiredCounts = [
    'received',
    'accepted',
    'blocked',
    'unknownMeters',
    'unchanged',
    'published',
    'conflicts',
  ]
  return (
    isRecord(result) &&
    result.persisted === true &&
    typeof result.ingestionId === 'string' &&
    result.ingestionId.length > 0 &&
    typeof result.checkpoint === 'string' &&
    isRecord(result.counts) &&
    Object.values(result.counts).every(
      (count) =>
        typeof count === 'number' && Number.isSafeInteger(count) && count >= 0,
    ) &&
    requiredCounts.every(
      (key) => isRecord(result.counts) && key in result.counts,
    )
  )
}

/** Provider-neutral transport: normalized readings only, without volume calculations. */
export class MeterReadingsClient {
  private token = ''

  constructor(
    private readonly baseUrl: string,
    private readonly clientId: string,
    private readonly clientSecret: string,
    private readonly fetcher: typeof fetch = fetch,
  ) {
    if (!baseUrl || !clientId || !clientSecret) {
      throw new Error(
        '[meter-readings] PLE_BASE_URL, CLIENT_ID and CLIENT_SECRET are required.',
      )
    }
  }

  async getContext(
    provider: string,
    scope: string,
  ): Promise<MeterStreamContext> {
    if (!provider.trim() || !scope.trim()) {
      throw new Error('[meter-readings] Provider and scope are required.')
    }

    const auth = await this.request('/service-accounts/token', {
      clientId: this.clientId,
      clientSecret: this.clientSecret,
    })
    const token = isRecord(auth)
      ? (auth.accessToken ?? auth.access_token ?? auth.token)
      : undefined
    if (typeof token !== 'string' || !token) {
      throw new Error('[meter-readings] Invalid PLE authentication response.')
    }

    this.token = token
    const context = await this.request(
      `/service-accounts/meter-streams?${new URLSearchParams({provider, scope}).toString()}`,
    )
    if (!isMeterStreamContext(context, provider, scope)) {
      throw new Error('[meter-readings] Invalid PLE stream context.')
    }

    return context
  }

  async ingest(batch: MeterIngestionBatch): Promise<MeterIngestionResult> {
    const result = await this.request(
      '/service-accounts/meter-readings/ingestions',
      batch,
    )
    if (
      !isMeterIngestionResult(result) ||
      result.checkpoint !== batch.windowEnd ||
      result.counts.received !== batch.readings.length
    ) {
      throw new Error(
        '[meter-readings] PLE did not acknowledge durable complete ingestion.',
      )
    }

    return result
  }

  private async request(
    path: string,
    body?: Record<string, unknown>,
  ): Promise<unknown> {
    let response: Response
    try {
      response = await this.fetcher(
        `${this.baseUrl.replace(/\/$/v, '')}${path}`,
        {
          method: body ? 'POST' : 'GET',
          headers: {
            Accept: 'application/json',
            ...(body ? {'Content-Type': 'application/json'} : {}),
            ...(this.token ? {Authorization: `Bearer ${this.token}`} : {}),
          },
          ...(body ? {body: JSON.stringify(body)} : {}),
          signal: AbortSignal.timeout(60_000),
          redirect: 'error',
        },
      )
    } catch {
      throw new Error(
        `[meter-readings] PLE request ${path} failed or timed out.`,
      )
    }

    if (!response.ok) {
      throw new Error(
        `[meter-readings] PLE request ${path}: HTTP ${response.status}.`,
      )
    }

    try {
      return (await response.json()) as unknown
    } catch {
      throw new Error('[meter-readings] PLE returned invalid JSON.')
    }
  }
}
