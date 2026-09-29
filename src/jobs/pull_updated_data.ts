import {type BaseConnector} from '../connectors/base-connector.js'
import {EvelerClient} from '../connectors/eveler.js'
import {type ServiceAccountPointContext} from '../connectors/types.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

export async function processPoint(parameters: {
  connectorRegistry: Map<string, BaseConnector<unknown, unknown>>
  partageonsLeauClient: PartageonsLeauClient
  serviceAccount: string
  serviceAccountToken: string
  declarantId: string
  contextId: string
  point: ServiceAccountPointContext
}): Promise<void> {
  const {
    connectorRegistry,
    partageonsLeauClient,
    serviceAccount,
    serviceAccountToken,
    declarantId,
    contextId,
    point,
  } = parameters

  const {
    pointId,
    exploitationId,
    countingCode,
    flowType,
    connector: connectorName,
    connectorId,
    connectorRate,
    sourcePointId,
    mostRecentAvailableDate,
    sourceFile,
    connectorParameters,
  } = point

  const connector = connectorRegistry.get(connectorName)

  if (connectorName === 'eveler' && !EvelerClient.isConfigured()) {
    console.log(
      '[PullUpdatedData] Eveler disabled: provider credentials are not configured.',
    )
    return
  }

  if (!connector) {
    console.error(
      `[PullUpdatedData] Connecteur introuvable pour le point source : ${sourcePointId} (connecteur : ${connectorName})`,
    )
    return
  }

  try {
    const output = await connector.run({
      serviceAccount,
      exploitationId,
      countingCode,
      flowType,
      sourcePointId,
      connectorId,
      rate: connectorRate,
      mostRecentAvailableDate,
      sourceFile,
      connectorParameters,
    })

    if (
      connectorName === 'eveler' &&
      output.data.metrics.every((metric) => metric.values.length === 0)
    ) {
      console.log('[PullUpdatedData] Eveler: no complete hours to ingest.')
      return
    }

    const acknowledgement = await partageonsLeauClient.ingest({
      output,
      pointId,
      declarantId,
      contextId,
      serviceAccountToken,
      ...(connectorName === 'eveler' && {requireAcknowledgement: true}),
    })

    if (connectorName === 'eveler') {
      if (!acknowledgement) {
        throw new Error(
          '[PullUpdatedData] Missing Eveler ingestion acknowledgement.',
        )
      }
      console.log(
        `[PullUpdatedData] Eveler acknowledged: imported=${acknowledgement.imported}, skippedValues=${acknowledgement.skippedValues}`,
      )
      return
    }

    console.log(
      `[PullUpdatedData] Données ingérées pour le point source : ${sourcePointId}`,
    )
  } catch (error) {
    if (connectorName === 'eveler') {
      throw error
    }
    console.error(
      `[PullUpdatedData] Échec de l'exécution du connecteur pour le point source ${sourcePointId} :`,
      error,
    )
  }
}

export async function pullUpdatedData(
  connectorRegistry: Map<string, BaseConnector<unknown, unknown>>,
) {
  console.log(
    '[PullUpdatedData] Démarrage du job de récupération de données mises à jour.',
  )

  const partageonsLeauClient = new PartageonsLeauClient()

  console.log('[PullUpdatedData] Recherche des comptes service disponibles...')

  const availableServiceAccounts =
    await partageonsLeauClient.getAvailableServiceAccounts()

  console.log(
    `[PullUpdatedData] Nombre de comptes service trouvés : ${availableServiceAccounts.length}`,
  )

  let evelerFailures = 0
  for (const serviceAccount of availableServiceAccounts) {
    console.log(`[PullUpdatedData] Auth service account : ${serviceAccount}`)

    const serviceAccountToken =
      await partageonsLeauClient.getServiceAccountToken(serviceAccount)

    const declarants =
      await partageonsLeauClient.getDeclarantsForServiceAccount(
        serviceAccount,
        serviceAccountToken,
      )

    console.log(
      `[PullUpdatedData] Nombre de déclarants pour ${serviceAccount} : ${declarants.length}`,
    )

    for (const declarant of declarants) {
      const contexts = await partageonsLeauClient.getContextsForDeclarant(
        declarant.id,
        serviceAccountToken,
      )

      const pointsCount = contexts.reduce(
        (total, context) => total + context.points.length,
        0,
      )
      if (pointsCount === 0) {
        continue
      }

      console.log(
        `[PullUpdatedData] Traitement déclarant ${declarant.id} (${declarant.name}), contextes=${contexts.length}, points=${pointsCount}`,
      )

      for (const context of contexts) {
        if (context.points.length === 0) {
          continue
        }

        console.log(
          `[PullUpdatedData] Contexte ${context.contextId} : ${context.points.length} points`,
        )

        for (const point of context.points) {
          try {
            await processPoint({
              connectorRegistry,
              partageonsLeauClient,
              serviceAccount,
              serviceAccountToken,
              declarantId: declarant.id,
              contextId: context.contextId,
              point,
            })
          } catch (error) {
            if (point.connector !== 'eveler') throw error
            evelerFailures++
            console.error('[PullUpdatedData] Eveler failed:', error)
          }
        }
      }
    }
  }
  if (evelerFailures > 0) {
    throw new Error(
      `[PullUpdatedData] ${evelerFailures} Eveler connector(s) failed.`,
    )
  }
}
