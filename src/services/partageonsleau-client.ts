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

/**
 * Client HTTP vers l’API « service account » Partageons l’eau (PLE), utilisé par
 * l’orchestrateur pour savoir quoi synchroniser et pour pousser les données
 * normalisées après exécution des connecteurs (Willie, Orange Live Objects, etc.).
 *
 * ## Flux normal (orchestration → PLE)
 *
 * Le job `pull-updated-data` enchaîne les appels suivants lorsque l’API est
 * configurée (`PLE_BASE_URL`, `CLIENT_ID`, `CLIENT_SECRET` tous renseignés) :
 *
 * 1. **`getAvailableServiceAccounts`** — Retourne `[CLIENT_ID]` : un seul compte
 *    service est piloté par les identifiants présents dans l’environnement.
 * 2. **`getServiceAccountToken`** — `POST /service-accounts/token` avec
 *    `clientId` / `clientSecret` → JWT compte service.
 * 3. **`getDeclarantsForServiceAccount`** — `GET /service-accounts/me/declarants`
 *    → liste des déclarants à traiter.
 * 4. Pour chaque déclarant :
 *    - **`getContextsForDeclarant`** — `GET .../declarants/:id/context`
 *      (Bearer = **JWT compte service** dans le job `pull_updated_data` actuel) →
 *      points d’exploitation, connecteur, paramètres (`sourcePointId`, `sourceFile`, …).
 *    - **`getDeclarantToken`** existe côté client si PLE ou un autre flux doit
 *      authentifier le contexte avec un JWT déclarant ; ce n’est pas utilisé
 *      aujourd’hui par `pull_updated_data`.
 * 5. Pour chaque point, l’orchestrateur exécute le connecteur puis appelle
 *    **`ingest`** — `POST /service-accounts/connectors/ingest` (Bearer = JWT
 *    compte service) avec le payload normalisé et des métadonnées (`point_id`,
 *    `declarant_id`, `context_id`, `last_run_at`).
 *
 * ## Mode hors API (développement)
 *
 * Si l’une des trois variables PLE manque, le client bascule sur des données
 * locales (`mock_responses.ts`) et des faux JWT ; `ingest` ne fait qu’un log.
 *
 * ## Réponses « contexte »
 *
 * `getContextsForDeclarant` accepte soit un corps avec `data[]` (format legacy),
 * soit `{ success, exploitations[] }` : dans ce cas un seul contexte synthétique
 * `declarant:<id>` regroupe tous les points qui ont un `connector.type` défini.
 */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

type DeclarantContextPayload = {
  contextId: string
  points: Array<{
    pointId: string
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
    mostRecentAvailableDate?: string | undefined
    connector?:
      | {
          type?: string | undefined
          parameters?: Record<string, unknown> | undefined
        }
      | undefined
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
      (point.pointId === undefined || typeof point.pointId === 'string') &&
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
      connector === null ||
      (isRecord(connector) &&
        (connector.type === undefined ||
          connector.type === null ||
          typeof connector.type === 'string') &&
        (connector.parameters === undefined ||
          connector.parameters === null ||
          isRecord(connector.parameters)))

    return (
      typeof exploitation.point.id === 'string' &&
      (exploitation.point.name === undefined ||
        typeof exploitation.point.name === 'string') &&
      (exploitation.mostRecentAvailableDate === undefined ||
        exploitation.mostRecentAvailableDate === null ||
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
  const granularityForApi = (granularity: Granularity): string => {
    switch (granularity) {
      case Granularity.FIFTEEN_MINUTES: {
        return '15 minutes'
      }

      case Granularity.HOUR: {
        return '1 hour'
      }

      case Granularity.DAY: {
        return '1 day'
      }

      case Granularity.WEEK: {
        return '1 week'
      }

      case Granularity.MONTH: {
        return '1 month'
      }

      case Granularity.YEAR: {
        return '1 year'
      }
    }
  }

  return {
    ...data,
    min_date: data.min_date?.toISOString(),
    max_date: data.max_date?.toISOString(),
    metrics: data.metrics.map((metric) => ({
      ...metric,
      granularity: granularityForApi(metric.granularity),
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

/** Voir le commentaire de module pour le flux orchestration ↔ PLE. */
export class PartageonsLeauClient {
  private readonly baseUrl = process.env.PLE_BASE_URL
  private readonly clientId = process.env.CLIENT_ID
  private readonly clientSecret = process.env.CLIENT_SECRET

  /**
   * Comptes service à parcourir. En API réelle : uniquement `CLIENT_ID`
   * (même identifiant que celui utilisé pour obtenir le token).
   */
  async getAvailableServiceAccounts(): Promise<string[]> {
    if (this.isApiConfigured() && this.clientId) {
      // En mode API réelle, on exécute l'orchestration sur le SA porté
      // par le couple clientId/clientSecret local.
      return [this.clientId]
    }

    return availableServiceAccounts
  }

  /** JWT compte service (Bearer pour `/me/*` et `connectors/ingest`). */
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

  /** Déclarants rattachés au compte service (étape 3 du flux). */
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

  /**
   * JWT déclarant (`POST .../declarants/:id/token`, Bearer = compte service).
   * Non utilisé par `pull_updated_data` pour l’instant ; à employer si l’API
   * exige un Bearer déclarant sur `GET .../context`.
   */
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

  /**
   * Points à synchroniser par connecteur. Mappe `exploitations[]` vers
   * `sourcePointId` / `connector` / `sourceFile` depuis `connector.parameters`.
   */
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
            pointId: point.pointId,
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
      console.warn(
        `[PartageonsLeauClient] Invalid context response for declarant ${declarantId}:`,
        JSON.stringify(response, null, 2),
      )

      return []
    }

    const points = response.exploitations
      .filter((exploitation) => {
        const connectorType = exploitation.connector?.type

        return typeof connectorType === 'string' && connectorType.length > 0
      })
      .map((exploitation) => {
        const connectorParameters = exploitation.connector?.parameters ?? {}
        const {sourceFile} = connectorParameters
        const {sourcePointId} = connectorParameters

        return {
          pointId: exploitation.point.id,
          sourcePointId:
            typeof sourcePointId === 'string'
              ? sourcePointId
              : exploitation.point.id,
          connector: exploitation.connector?.type ?? '',
          mostRecentAvailableDate: toOptionalDate(
            exploitation.mostRecentAvailableDate ?? undefined,
          ),
          sourceFile: typeof sourceFile === 'string' ? sourceFile : undefined,
        }
      })

    if (points.length > 0) {
      console.log(
        `[PartageonsLeauClient] Declarant ${declarantId}: exploitations=${response.exploitations.length}, connector points=${points.length}`,
      )
    }

    return [
      {
        contextId: `declarant:${declarantId}`,
        points,
      },
    ]
  }

  /**
   * Envoie le résultat d’un connecteur vers PLE (étape 5 du flux).
   * Normalise d’abord les séries temporelles (alignement granularité, agrégation
   * selon `MetricType`) puis sérialise les dates en ISO pour le JSON.
   */
  async ingest(parameters: {
    output: ConnectorOutput
    pointId: string
    declarantId: string
    contextId: string
    serviceAccountToken: string
  }): Promise<void> {
    const {output, pointId, declarantId, contextId, serviceAccountToken} =
      parameters

    const normalizedData = normalizePayloadData({
      ...output.data,
    })

    const metricCount = normalizedData.metrics.length
    const valueCount = normalizedData.metrics.reduce(
      (total, metric) => total + metric.values.length,
      0,
    )

    const serializedOutput = serializeOutputForPost(output, normalizedData)

    const payload = {
      ...serializedOutput,
      metadata: {
        point_id: pointId,
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
      '/service-accounts/connectors/ingest',
      payload,
      serviceAccountToken,
    )
  }

  /** API PLE réelle si base URL + identifiants OAuth compte service sont définis. */
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
