import {type BaseConnector} from '../connectors/base-connector.js'
import {type ServiceAccountPointContext} from '../connectors/types.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

async function processPoint(parameters: {
  connectorRegistry: Map<string, BaseConnector<unknown, unknown>>
  partageonsLeauClient: PartageonsLeauClient
  serviceAccount: string
  declarantId: string
  contextId: string
  declarantToken: string
  point: ServiceAccountPointContext
}): Promise<void> {
  const {
    connectorRegistry,
    partageonsLeauClient,
    serviceAccount,
    declarantId,
    contextId,
    declarantToken,
    point,
  } = parameters

  const {
    connector: connectorName,
    sourcePointId,
    mostRecentAvailableDate,
    sourceFile,
  } = point
  const connector = connectorRegistry.get(connectorName)

  if (!connector) {
    console.error(
      `[PullUpdatedData] Connecteur introuvable pour le point source : ${sourcePointId} (connecteur : ${connectorName})`,
    )
    return
  }

  try {
    const output = await connector.run({
      serviceAccount,
      sourcePointId,
      mostRecentAvailableDate,
      sourceFile,
    })

    await partageonsLeauClient.ingest(output)
    await partageonsLeauClient.updatePointLastRunAt({
      declarantId,
      contextId,
      sourcePointId,
      lastRunAt: output.generatedAt,
      declarantToken,
    })
    console.log(
      `[PullUpdatedData] Données ingérées et last_run_at mis à jour pour le point source : ${sourcePointId}`,
    )
  } catch (error) {
    console.error(
      `[PullUpdatedData] Échec de l'exécution du connecteur pour le point source ${sourcePointId} :`,
      error,
    )
  }
}

/**
 * Effectue une synchronisation des données pour chaque compte service via les connecteurs disponibles.
 */
export async function pullUpdatedData(
  connectorRegistry: Map<string, BaseConnector<unknown, unknown>>,
) {
  console.log(
    '[PullUpdatedData] Démarrage du job de récupération de données mises à jour.',
  )

  const partageonsLeauClient = new PartageonsLeauClient()

  // Récupère la liste des comptes service disponibles
  console.log('[PullUpdatedData] Recherche des comptes service disponibles...')

  const availableServiceAccounts =
    await partageonsLeauClient.getAvailableServiceAccounts()
  console.log(
    `[PullUpdatedData] Nombre de comptes service trouvés : ${availableServiceAccounts.length}`,
  )

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
      console.log(
        `[PullUpdatedData] Traitement déclarant ${declarant.id} (${declarant.name})`,
      )
      const declarantToken = await partageonsLeauClient.getDeclarantToken(
        declarant.id,
        serviceAccountToken,
      )
      const contexts = await partageonsLeauClient.getContextsForDeclarant(
        declarant.id,
        declarantToken,
      )

      console.log(
        `[PullUpdatedData] Nombre de contextes pour le déclarant ${declarant.id} : ${contexts.length}`,
      )

      for (const context of contexts) {
        console.log(
          `[PullUpdatedData] Contexte ${context.contextId} : ${context.points.length} points`,
        )

        for (const point of context.points) {
          await processPoint({
            connectorRegistry,
            partageonsLeauClient,
            serviceAccount,
            declarantId: declarant.id,
            contextId: context.contextId,
            declarantToken,
            point,
          })
        }
      }
    }
  }
}
