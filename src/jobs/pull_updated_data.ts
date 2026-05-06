import {type BaseConnector} from '../connectors/base-connector.js'
import {type ServiceAccountPointContext} from '../connectors/types.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

async function processPoint(parameters: {
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

    await partageonsLeauClient.ingest({
      output,
      pointId,
      declarantId,
      contextId,
      serviceAccountToken,
    })

    console.log(
      `[PullUpdatedData] Données ingérées pour le point source : ${sourcePointId}`,
    )
  } catch (error) {
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
          await processPoint({
            connectorRegistry,
            partageonsLeauClient,
            serviceAccount,
            serviceAccountToken,
            declarantId: declarant.id,
            contextId: context.contextId,
            point,
          })
        }
      }
    }
  }
}
