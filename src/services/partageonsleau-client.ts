import type {
  DeclarantContext,
  ConnectorOutput,
} from '../connectors/types.js'
import {
  availableServiceAccounts,
  contextsByDeclarant,
  declarantsByServiceAccount,
  type MockDeclarant,
} from './mock_responses.js'

export class PartageonsLeauClient {
  private readonly baseUrl = process.env.PLE_BASE_URL
  private readonly clientId = process.env.CLIENT_ID
  private readonly clientSecret = process.env.CLIENT_SECRET

  private isApiConfigured(): boolean {
    return Boolean(this.baseUrl && this.clientId && this.clientSecret)
  }

  async getAvailableServiceAccounts(): Promise<string[]> {
    return availableServiceAccounts
  }

  async getServiceAccountToken(
    serviceAccount: string,
  ): Promise<string> {
    if (!this.isApiConfigured()) {
      return `mock-sa-token:${serviceAccount}`
    }

    const response = await this.postJson<{
      access_token?: string
      token?: string
    }>('/service-accounts/token', {
      clientId: this.clientId,
      clientSecret: this.clientSecret,
    })

    const token = response.access_token ?? response.token
    if (!token) {
      throw new Error('[PartageonsLeauClient] Missing token in service account auth response.')
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

    const response = await this.getJson<{data?: MockDeclarant[]}>(
      '/service-accounts/me/declarants',
      serviceAccountToken,
    )

    return response.data ?? []
  }

  async getDeclarantToken(
    declarantId: string,
    serviceAccountToken: string,
  ): Promise<string> {
    if (!this.isApiConfigured()) {
      return `mock-declarant-token:${declarantId}`
    }

    const response = await this.postJson<{
      access_token?: string
      token?: string
    }>(
      `/service-accounts/declarants/${encodeURIComponent(declarantId)}/token`,
      {},
      serviceAccountToken,
    )

    const token = response.access_token ?? response.token
    if (!token) {
      throw new Error(`[PartageonsLeauClient] Missing token in declarant auth response for "${declarantId}".`)
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

    const response = await this.getJson<{data?: DeclarantContext[]}>(
      '/service-accounts/me/contexts',
      declarantToken,
    )
    return response.data ?? []
  }

  private async getJson<T>(path: string, bearerToken: string): Promise<T> {
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

    return response.json() as Promise<T>
  }

  private async postJson<T>(
    path: string,
    body: Record<string, unknown>,
    bearerToken?: string,
  ): Promise<T> {
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

    return response.json() as Promise<T>
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
}
