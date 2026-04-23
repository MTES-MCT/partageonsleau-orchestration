import {
  Granularity,
  MetricType,
  type DeclarantContext,
  type ConnectorOutput,
  type ParsedPointPayload,
  type Timeserie,
  type TimeserieValue,
} from '../connectors/types.js'
import {
  availableServiceAccounts,
  contextsByDeclarant,
  declarantsByServiceAccount,
  type MockDeclarant,
} from './mock_responses.js'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

type DeclarantContextPayload = {
  contextId: string
  points: Array<{
    sourcePointId: string
    connector: string
    mostRecentAvailableDate: string | undefined
    sourceFile: string
  }>
}

type ServiceAccountDeclarantsResponse = {
  data: Array<{
    declarantUserId: string
    declarantName?: string
  }>
}

type DeclarantContextApiResponse = {
  success: boolean
  exploitations: Array<{
    point: {
      id: string
      name?: string
    }
    mostRecentAvailableDate?: string
    connector?: {
      type?: string
      parameters?: Record<string, unknown>
    }
  }>
}

function isDeclarantContextPayload(
  value: unknown,
): value is DeclarantContextPayload {
  if (!isRecord(value) || typeof value.contextId !== 'string') {
    return false
  }

  if (!Array.isArray(value.points)) {
    return false
  }

  return value.points.every((point) => {
    return (
      isRecord(point) &&
      typeof point.sourcePointId === 'string' &&
      typeof point.connector === 'string' &&
      (point.mostRecentAvailableDate === undefined ||
        typeof point.mostRecentAvailableDate === 'string') &&
      (point.sourceFile === undefined || typeof point.sourceFile === 'string')
    )
  })
}

function isServiceAccountDeclarantsResponse(
  value: unknown,
): value is ServiceAccountDeclarantsResponse {
  if (!isRecord(value) || !Array.isArray(value.data)) {
    return false
  }

  return value.data.every((item) => {
    return (
      isRecord(item) &&
      typeof item.declarantUserId === 'string' &&
      (item.declarantName === undefined ||
        typeof item.declarantName === 'string')
    )
  })
}

function isDeclarantContextApiResponse(
  value: unknown,
): value is DeclarantContextApiResponse {
  if (
    !isRecord(value) ||
    typeof value.success !== 'boolean' ||
    !Array.isArray(value.exploitations)
  ) {
    return false
  }

  return value.exploitations.every((exploitation) => {
    if (!isRecord(exploitation) || !isRecord(exploitation.point)) {
      return false
    }

    const {connector} = exploitation
    const hasValidConnector =
      connector === undefined ||
      (isRecord(connector) &&
        (connector.type === undefined || typeof connector.type === 'string') &&
        (connector.parameters === undefined || isRecord(connector.parameters)))

    return (
      typeof exploitation.point.id === 'string' &&
      (exploitation.point.name === undefined ||
        typeof exploitation.point.name === 'string') &&
      (exploitation.mostRecentAvailableDate === undefined ||
        typeof exploitation.mostRecentAvailableDate === 'string') &&
      hasValidConnector
    )
  })
}

function toOptionalDate(value: string | undefined): Date | undefined {
  if (!value) {
    return undefined
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return undefined
  }

  return parsed
}

function alignDateToGranularity(date: Date, granularity: Granularity): Date {
  const aligned = new Date(date)

  switch (granularity) {
    case Granularity.FIFTEEN_MINUTES: {
      aligned.setUTCSeconds(0, 0)
      const minutes = aligned.getUTCMinutes()
      aligned.setUTCMinutes(Math.floor(minutes / 15) * 15)
      return aligned
    }

    case Granularity.HOUR: {
      aligned.setUTCMinutes(0, 0, 0)

      return aligned
    }

    case Granularity.DAY: {
      aligned.setUTCHours(0, 0, 0, 0)

      return aligned
    }

    case Granularity.WEEK: {
      aligned.setUTCHours(0, 0, 0, 0)
      const dayOfWeek = aligned.getUTCDay() // 0=Sunday, 1=Monday, ...
      const diffToMonday = (dayOfWeek + 6) % 7
      aligned.setUTCDate(aligned.getUTCDate() - diffToMonday)

      return aligned
    }

    case Granularity.MONTH: {
      aligned.setUTCDate(1)
      aligned.setUTCHours(0, 0, 0, 0)

      return aligned
    }

    case Granularity.YEAR: {
      aligned.setUTCMonth(0, 1)
      aligned.setUTCHours(0, 0, 0, 0)

      return aligned
    }
  }
}

type BucketAggregator = (
  existing: TimeserieValue,
  candidate: TimeserieValue,
) => TimeserieValue

const metricBucketAggregators: Record<MetricType, BucketAggregator> = {
  [MetricType.VOLUME_PRELEVE](existing, candidate) {
    // Une valeur de volume est additive dans un même bucket temporel.
    return {
      date: existing.date,
      value: existing.value + candidate.value,
    }
  },
  [MetricType.INDEX](_existing, candidate) {
    // Un index est un état instantané: on conserve la dernière valeur observée du bucket.
    return candidate
  },
}

function mergeValuesInBucket(
  metricType: MetricType,
  existing: TimeserieValue | undefined,
  candidate: TimeserieValue,
): TimeserieValue {
  if (!existing) {
    return candidate
  }

  return metricBucketAggregators[metricType](existing, candidate)
}

/**
 * Aligne les timestamps sur la granularite de la metrique, puis fusionne
 * les collisions dans un meme bucket via la strategie d'agregation du MetricType.
 */
function normalizeTimeserieValues(metric: Timeserie): TimeserieValue[] {
  const datedValues = metric.values
    .map((value) => {
      const parsedDate = new Date(value.date)
      return {
        parsedDate,
        value,
      }
    })
    .filter((entry) => !Number.isNaN(entry.parsedDate.getTime()))

  const sortedValues: typeof datedValues = []
  for (const entry of datedValues) {
    const insertIndex = sortedValues.findIndex(
      (current) => current.parsedDate.getTime() > entry.parsedDate.getTime(),
    )
    if (insertIndex === -1) {
      sortedValues.push(entry)
    } else {
      sortedValues.splice(insertIndex, 0, entry)
    }
  }

  const valuesByBucket = new Map<number, TimeserieValue>()
  for (const entry of sortedValues) {
    const alignedDate = alignDateToGranularity(
      entry.parsedDate,
      metric.granularity,
    )
    const bucketKey = alignedDate.getTime()
    const candidate: TimeserieValue = {
      date: alignedDate,
      value: entry.value.value,
    }
    const merged = mergeValuesInBucket(
      metric.type,
      valuesByBucket.get(bucketKey),
      candidate,
    )
    valuesByBucket.set(bucketKey, merged)
  }

  const sortedEntries: Array<[number, TimeserieValue]> = []
  for (const entry of valuesByBucket.entries()) {
    const insertIndex = sortedEntries.findIndex(([key]) => key > entry[0])
    if (insertIndex === -1) {
      sortedEntries.push(entry)
    } else {
      sortedEntries.splice(insertIndex, 0, entry)
    }
  }

  return sortedEntries.map(([, value]) => value)
}

function normalizePayloadData(data: ParsedPointPayload): ParsedPointPayload {
  const normalizedMetrics = data.metrics.map((metric) => ({
    ...metric,
    values: normalizeTimeserieValues(metric),
  }))

  const allMetricDates = normalizedMetrics.flatMap((metric) =>
    metric.values.map((value) => value.date),
  )

  return {
    ...data,
    metrics: normalizedMetrics,
    min_date: allMetricDates.length > 0 ? allMetricDates[0] : data.min_date,
    max_date: allMetricDates.length > 0 ? allMetricDates.at(-1) : data.max_date,
  }
}

function serializePayloadDataForPost(
  data: ParsedPointPayload,
): Record<string, unknown> {
  return {
    ...data,
    min_date: data.min_date?.toISOString(),
    max_date: data.max_date?.toISOString(),
    metrics: data.metrics.map((metric) => ({
      ...metric,
      values: metric.values.map((value) => ({
        ...value,
        date: value.date.toISOString(),
      })),
    })),
  }
}

function serializeOutputForPost(
  output: ConnectorOutput,
  normalizedData: ParsedPointPayload,
): Record<string, unknown> {
  return {
    ...output,
    lastRunAt: output.lastRunAt.toISOString(),
    data: serializePayloadDataForPost(normalizedData),
  }
}

export class PartageonsLeauClient {
  private readonly baseUrl = process.env.PLE_BASE_URL
  private readonly clientId = process.env.CLIENT_ID
  private readonly clientSecret = process.env.CLIENT_SECRET

  async getAvailableServiceAccounts(): Promise<string[]> {
    if (this.isApiConfigured() && this.clientId) {
      // En mode API réelle, on exécute l'orchestration sur le SA porté
      // par le couple clientId/clientSecret local.
      return [this.clientId]
    }

    return availableServiceAccounts
  }

  async getServiceAccountToken(serviceAccount: string): Promise<string> {
    if (!this.isApiConfigured()) {
      return `mock-sa-token:${serviceAccount}`
    }

    const response = await this.postJson('/service-accounts/token', {
      clientId: this.clientId,
      clientSecret: this.clientSecret,
    })

    if (!isRecord(response)) {
      throw new Error(
        '[PartageonsLeauClient] Invalid service account token response.',
      )
    }

    const {
      accessToken,
      access_token: legacyAccessToken,
      token: fallbackToken,
    } = response
    const token =
      (typeof accessToken === 'string' && accessToken) ||
      (typeof legacyAccessToken === 'string' && legacyAccessToken) ||
      (typeof fallbackToken === 'string' && fallbackToken)
    if (!token) {
      throw new Error(
        '[PartageonsLeauClient] Missing token in service account auth response.',
      )
    }

    return token
  }

  async getDeclarantsForServiceAccount(
    serviceAccount: string,
    serviceAccountToken: string,
  ): Promise<MockDeclarant[]> {
    if (!this.isApiConfigured()) {
      return declarantsByServiceAccount[serviceAccount] ?? []
    }

    const response = await this.getJson(
      '/service-accounts/me/declarants',
      serviceAccountToken,
    )

    if (!isServiceAccountDeclarantsResponse(response)) {
      return []
    }

    return response.data.map((item) => ({
      id: item.declarantUserId,
      name: item.declarantName ?? item.declarantUserId,
    }))
  }

  async getDeclarantToken(
    declarantId: string,
    serviceAccountToken: string,
  ): Promise<string> {
    if (!this.isApiConfigured()) {
      return `mock-declarant-token:${declarantId}`
    }

    const response = await this.postJson(
      `/service-accounts/declarants/${encodeURIComponent(declarantId)}/token`,
      {},
      serviceAccountToken,
    )

    if (!isRecord(response)) {
      throw new Error(
        '[PartageonsLeauClient] Invalid declarant token response.',
      )
    }

    const {
      accessToken,
      access_token: legacyAccessToken,
      token: fallbackToken,
    } = response
    const token =
      (typeof accessToken === 'string' && accessToken) ||
      (typeof legacyAccessToken === 'string' && legacyAccessToken) ||
      (typeof fallbackToken === 'string' && fallbackToken)
    if (!token) {
      throw new Error(
        `[PartageonsLeauClient] Missing token in declarant auth response for "${declarantId}".`,
      )
    }

    return token
  }

  async getContextsForDeclarant(
    declarantId: string,
    declarantToken: string,
  ): Promise<DeclarantContext[]> {
    if (!this.isApiConfigured()) {
      return contextsByDeclarant[declarantId] ?? []
    }

    const response = await this.getJson(
      `/service-accounts/declarants/${encodeURIComponent(declarantId)}/context`,
      declarantToken,
    )
    // Ancien format conservé pour compatibilité montante.
    if (isRecord(response) && Array.isArray(response.data)) {
      return response.data
        .filter((item): item is DeclarantContextPayload =>
          isDeclarantContextPayload(item),
        )
        .map((context) => ({
          contextId: context.contextId,
          points: context.points.map((point) => ({
            sourcePointId: point.sourcePointId,
            connector: point.connector,
            mostRecentAvailableDate: toOptionalDate(
              point.mostRecentAvailableDate,
            ),
            sourceFile: point.sourceFile,
          })),
        }))
    }

    if (!isDeclarantContextApiResponse(response)) {
      return []
    }

    return [
      {
        contextId: `declarant:${declarantId}`,
        points: response.exploitations
          .filter(
            (exploitation) =>
              typeof exploitation.connector?.type === 'string' &&
              exploitation.connector.type.length > 0,
          )
          .map((exploitation) => {
            const sourceFile = exploitation.connector?.parameters?.sourceFile
            return {
              sourcePointId: exploitation.point.id,
              connector: exploitation.connector?.type ?? '',
              mostRecentAvailableDate: toOptionalDate(
                exploitation.mostRecentAvailableDate,
              ),
              sourceFile:
                typeof sourceFile === 'string' ? sourceFile : undefined,
            }
          }),
      },
    ]
  }

  /**
   * Endpoint Partageons l'eau cible (a implementer plus tard):
   *
   * But:
   * - Envoyer le resultat normalise d'un connecteur pour ingestion,
   *   avec les metadonnees de synchronisation (dont last_run_at).
   *
   */
  async ingest(parameters: {
    output: ConnectorOutput
    declarantId: string
    contextId: string
    declarantToken: string
  }): Promise<void> {
    const {output, declarantId, contextId, declarantToken} = parameters

    const normalizedData = normalizePayloadData(output.data)

    // TODO: remplacer par le POST d'ingestion vers la plateforme.
    const metricCount = normalizedData.metrics.length
    const valueCount = normalizedData.metrics.reduce(
      (total, metric) => total + metric.values.length,
      0,
    )
    const serializedOutput = serializeOutputForPost(output, normalizedData)
    console.log(JSON.stringify(serializedOutput, null, 2))
    const payload = {
      ...serializedOutput,
      metadata: {
        declarant_id: declarantId,
        context_id: contextId,
        last_run_at: output.lastRunAt.toISOString(),
      },
    }

    if (!this.isApiConfigured()) {
      console.log(
        `[PartageonsLeauClient] Ingesting ${metricCount} metrics (${valueCount} values) for service account: ${output.serviceAccount} and source point: ${output.sourcePointId} with last_run_at=${output.lastRunAt.toISOString()}.`,
      )
      return
    }

    await this.postJson(
      '/service-accounts/declarants/ingest',
      payload,
      declarantToken,
    )
  }

  private isApiConfigured(): boolean {
    return Boolean(this.baseUrl && this.clientId && this.clientSecret)
  }

  private async getJson(path: string, bearerToken: string): Promise<unknown> {
    if (!this.baseUrl) {
      throw new Error('[PartageonsLeauClient] Missing PLE_BASE_URL.')
    }

    const response = await fetch(`${this.baseUrl}${path}`, {
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${bearerToken}`,
      },
    })

    if (!response.ok) {
      const body = await response.text()
      throw new Error(
        `[PartageonsLeauClient] GET ${path} failed with status ${response.status}: ${body}`,
      )
    }

    return response.json()
  }

  private async postJson(
    path: string,
    body: Record<string, unknown>,
    bearerToken?: string,
  ): Promise<unknown> {
    if (!this.baseUrl) {
      throw new Error('[PartageonsLeauClient] Missing PLE_BASE_URL.')
    }

    const headers: Record<string, string> = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }
    if (bearerToken) {
      headers.Authorization = `Bearer ${bearerToken}`
    }

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    })

    if (!response.ok) {
      const responseBody = await response.text()
      throw new Error(
        `[PartageonsLeauClient] POST ${path} failed with status ${response.status}: ${responseBody}`,
      )
    }

    return response.json()
  }
}
