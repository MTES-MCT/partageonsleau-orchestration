import type {
  ConnectorOutput,
  ServiceAccountContext,
} from '../connectors/types.js'
import {availableServiceAccounts, contextByAccount} from './mock_responses.js'

export class PartageonsLeauClient {
  async getAvailableServiceAccounts(): Promise<string[]> {
    return availableServiceAccounts
  }

  /**
   * Endpoint Partageons l'eau cible (a implementer plus tard):
   *
   * But:
   * - Recuperer le contexte d'execution pour un service account.
   * - Permettre un run incremental (lastRunAt) sur les points autorises.
   *
   */
  async getContextForServiceAccount(
    serviceAccount: string,
  ): Promise<ServiceAccountContext | undefined> {
    // Implementation locale initiale en attendant l'integration API Partageons l'eau.
    const context = contextByAccount[serviceAccount]
    if (!context) {
      return undefined
    }

    return {
      serviceAccount,
      points: context.points,
    }
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
