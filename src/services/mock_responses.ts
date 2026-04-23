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
          pointId: '392e70c3-f3ba-456a-952e-697a28f7da9d',
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
          pointId: '013219a3-8ab0-4ae4-ac0b-8fa00c9af71a',
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
          pointId: '0f69a233-d805-4d36-b524-6a64fe34ba89',
          sourcePointId: '38-0852',
          connector: 'aquasys',
          mostRecentAvailableDate: undefined,
          sourceFile: 'data/Dossiers_Consommations_30092024-31122025.xlsx',
        },
        {
          pointId: '0f69a233-d805-4d36-b524-6a64fe34ba90',
          sourcePointId: '38-0854',
          connector: 'aquasys',
          mostRecentAvailableDate: undefined,
          sourceFile: 'data/Dossiers_Consommations_30092024-31122025.xlsx',
        },
      ],
    },
    {
      contextId: 'template_file_blv_0',
      points: [
        {
          pointId: '7c0b8d86-113e-4e8f-8dca-60af3bd95811',
          sourcePointId: "Captages de l'Iles (Manthes) - Forage profond",
          connector: 'template_file',
          mostRecentAvailableDate: undefined,
          sourceFile: 'data/declaration_valloire_gallaure_11_2025.xlsx',
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
