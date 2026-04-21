import {type BaseConnector} from '../connectors/base-connector.js'
import {PartageonsLeauClient} from '../services/partageonsleau-client.js'

/**
 * Effectue une synchronisation des données pour chaque compte service via les connecteurs disponibles.
 */
export async function pullUpdatedData(
  connectorRegistry: Map<string, BaseConnector>,
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
    console.log(
      `[PullUpdatedData] Récupération du contexte pour le compte service : ${serviceAccount}`,
    )

    const context =
      await partageonsLeauClient.getContextForServiceAccount(serviceAccount)

    if (!context) {
      console.error(
        `[PullUpdatedData] Contexte introuvable pour le compte service : ${serviceAccount}`,
      )
      continue
    }

    console.log(
      `[PullUpdatedData] Nombre de points à traiter pour ${serviceAccount} : ${context.points.length}`,
    )

    for (const point of context.points) {
      const {
        connector: connectorName,
        sourcePointId,
        lastRunAt,
        most_recent_available_date,
        sourceFiles,
      } = point
      const connector = connectorRegistry.get(connectorName)

      if (!connector) {
        console.error(
          `[PullUpdatedData] Connecteur introuvable pour le point source : ${sourcePointId} (connecteur : ${connectorName})`,
        )
        continue
      }

      try {
        const output = await connector.run({
          serviceAccount,
          sourcePointId,
          lastRunAt,
          most_recent_available_date,
          sourceFiles,
        })

        await partageonsLeauClient.ingest(output)
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
  }
}
