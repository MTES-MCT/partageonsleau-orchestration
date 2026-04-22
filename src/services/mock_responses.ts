import type {DeclarantContext} from '../connectors/types.js'

export type MockDeclarant = {
  id: string
  name: string
}

export const declarantsByServiceAccount: Record<string, MockDeclarant[]> = {
  service_account_primaire: [
    {
      id: 'decl_blv_0',
      name: 'BLV Declarant 0',
    },
  ],
}

export const availableServiceAccounts: string[] = Object.keys(
  declarantsByServiceAccount,
)

export const contextsByDeclarant: Record<string, DeclarantContext[]> = {
  decl_blv_0: [
    {
      contextId: 'willie_blv_0',
      points: [
        {
          sourcePointId: 'aedd02ee-6876-4afc-91bc-b2a9a142b79f',
          connector: 'willie',
          mostRecentAvailableDate: undefined,
        },
      ],
    },
    {
      contextId: 'orange_live_objects_blv_0',
      points: [
        {
          sourcePointId: 'urn:lo:nsid:imei:359404232376831',
          connector: 'orange_live_objects',
          mostRecentAvailableDate: undefined,
        },
      ],
    },
    {
      contextId: 'aquasys_blv_0',
      points: [
        {
          sourcePointId: '38-0852',
          connector: 'aquasys',
          mostRecentAvailableDate: undefined,
          sourceFile: 'data/Dossiers_Consommations_30092024-31122025.xlsx',
        },
        {
          sourcePointId: '38-0854',
          connector: 'aquasys',
          mostRecentAvailableDate: undefined,
          sourceFile: 'data/Dossiers_Consommations_30092024-31122025.xlsx',
        },
      ],
    },
  ],
  decl_empty_0: [
    {
      contextId: 'willie_blv_1',
      points: [],
    },
  ],
}
