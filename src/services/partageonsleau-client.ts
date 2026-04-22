import type {DeclarantContext, ConnectorOutput} from '../connectors/types.js'
import {
  availableServiceAccounts,
  contextsByDeclarant,
  declarantsByServiceAccount,
  type MockDeclarant,
} from './mock_responses.js'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isMockDeclarant(value: unknown): value is MockDeclarant {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.name === 'string'
  )
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

export class PartageonsLeauClient {
  private readonly baseUrl = process.env.PLE_BASE_URL
  private readonly clientId = process.env.CLIENT_ID
  private readonly clientSecret = process.env.CLIENT_SECRET

  async getAvailableServiceAccounts(): Promise<string[]> {
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

    const accessToken = response.access_token
    const fallbackToken = response.token
    const token =
      (typeof accessToken === 'string' && accessToken) ||
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

    if (!isRecord(response) || !Array.isArray(response.data)) {
      return []
    }

    return response.data.filter((item): item is MockDeclarant =>
      isMockDeclarant(item),
    )
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

    const accessToken = response.access_token
    const fallbackToken = response.token
    const token =
      (typeof accessToken === 'string' && accessToken) ||
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
    if (!isRecord(response) || !Array.isArray(response.data)) {
      return []
    }

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

  async updatePointLastRunAt(parameters: {
    declarantId: string
    contextId: string
    sourcePointId: string
    lastRunAt: string
    declarantToken: string
  }): Promise<void> {
    if (!this.isApiConfigured()) {
      console.log(
        `[PartageonsLeauClient] Mock update last_run_at=${parameters.lastRunAt} for point ${parameters.sourcePointId} in context ${parameters.contextId} (declarant ${parameters.declarantId}).`,
      )
      return
    }

    await this.postJson(
      `/service-accounts/declarants/${encodeURIComponent(parameters.declarantId)}/context/${encodeURIComponent(parameters.contextId)}/points/${encodeURIComponent(parameters.sourcePointId)}/last-run`,
      {
        last_run_at: parameters.lastRunAt,
      },
      parameters.declarantToken,
    )
  }

  /**
   * Endpoint Partageons l'eau cible (a implementer plus tard):
   *
   * But:
   * - Envoyer le resultat normalise d'un connecteur pour ingestion.
   * - La payload est celle produite par la pipeline ConnectorOutput.
   *
   */
  async ingest(output: ConnectorOutput): Promise<void> {
    // TODO: remplacer par le POST d'ingestion vers la plateforme.
    const metricCount = output.data.metrics.length
    const valueCount = output.data.metrics.reduce(
      (total, metric) => total + metric.values.length,
      0,
    )
    console.log(
      `[PartageonsLeauClient] Ingesting ${metricCount} metrics (${valueCount} values) for service account: ${output.serviceAccount} and source point: ${output.sourcePointId}`,
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
