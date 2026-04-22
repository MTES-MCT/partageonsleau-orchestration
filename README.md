# partageonsleau-orchestration

Orchestrateur TypeScript pour recuperer des donnees metier (connecteurs externes), les normaliser, puis les envoyer vers la plateforme Partageons l'eau (PLE).

## Objectif

Le job principal:

1. recupere la liste des service accounts disponibles
2. genere un JWT du service account
3. recupere la liste des declarants autorises
4. pour chaque declarant, genere un JWT declarant puis recupere ses contextes
5. execute le connecteur associe pour chaque point de chaque contexte
6. transforme la donnee vers un format commun puis envoie le resultat vers PLE

## Stack

- Node.js + TypeScript (ESM)
- Execution locale en TS via `tsx`
- Connecteurs implantes: `willie` (autres connecteurs en stub)

## Installation

```bash
npm install
```

## Configuration

Copier `.env.example` vers `.env` puis renseigner les variables.

Variables actuellement utilisees:

- `WILLIE_API_TOKEN`: token Bearer pour l'API Willie
- `ORANGE_LIVE_OBJECTS_API_KEY`: cle API pour l'API Orange Live Objects
- `PLE_BASE_URL`: URL de base de l'API Partageons l'eau (optionnel tant que le mode mock est actif)
- `CLIENT_ID`: identifiant client pour generer le JWT service account
- `CLIENT_SECRET`: secret client pour generer le JWT service account

## Scripts

- `npm run dev`: lance en TypeScript directement (avec `.env`)
- `npm run check`: verifie les types TypeScript
- `npm run build`: compile en `dist/`
- `npm run start`: lance la version compilee

## Architecture

- `index.ts`: point d'entree
- `src/jobs/pull_updated_data.ts`: orchestration du job principal
- `src/connectors/base-connector.ts`: pipeline commun d'un connecteur
- `src/connectors/willie.ts`: implementation Willie
- `src/connectors/types.ts`: types partages (contexte, payload standardise)
- `src/services/partageonsleau-client.ts`: client PLE (mock local pour l'instant)
- `src/services/mock_responses.ts`: donnees locales temporaires pour simuler les reponses PLE

## Contrat de sortie connecteur

Chaque connecteur retourne un payload standardise par point:

- `id_point_de_prelevement`
- `metrics[]` avec:
  - `type` (`index` ou `volume_preleve`)
  - `frequency` (ex: `day`)
  - `values[]` (`date`, `value`)
  - `unit` (actuellement `m3` ou `null`)

## Willie (etat actuel)

Le connecteur Willie interroge:

- `GET https://api.meetwillie.com/v1/stations/consumption`

Parametres utilises:

- `stationIds` = `sourcePointId` (station_id Willie)
- `startDate` = `lastRunAt` (ou fallback)
- `endDate` = maintenant
- `resolution` = `day`

La reponse `stations[].datapoints[]` est convertie au format standardise.

## Prochaines etapes recommandees

- Remplacer les mocks PLE par de vrais appels API
- Rendre la frequence configurable depuis le contexte PLE
- Ajouter des tests unitaires par connecteur
- Stabiliser et partager un package de contrats entre ce repo et PLE
