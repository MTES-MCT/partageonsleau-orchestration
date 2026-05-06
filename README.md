# partageonsleau-orchestration

Orchestrateur TypeScript pour récupérer des données métier (connecteurs externes), les normaliser, puis les envoyer vers la plateforme Partageons l'eau (PLE). Le service expose une API HTTP, planifie des jobs et traite les webhooks PLE via **Redis** et **BullMQ**.

## Objectif du job « pull »

Le job `pull-updated-data` enchaîne :

1. récupération des comptes service concernés (mock local ou API PLE selon la configuration) ;
2. génération du JWT compte service ;
3. liste des déclarants autorisés ;
4. pour chaque déclarant, JWT déclarant puis récupération des contextes / points ;
5. exécution du connecteur associé à chaque point ;
6. normalisation du payload et envoi vers PLE (`ingest`) lorsque l’API est configurée.

Un second job, `process-declaration`, traite les déclarations déposées sur PLE (fichiers, points associés) après réception d’un webhook signé.

## Stack

- **Node.js** 24.x (voir `engines` dans `package.json`) + **TypeScript** (ESM, `NodeNext`)
- **Express** 5 — API HTTP et webhooks
- **BullMQ** + **ioredis** — files d’attente et workers
- **Sentry** (`@sentry/node`, profiling) — erreurs et traces (optionnel via `SENTRY_DSN`)
- **moment**, **xlsx** — utilitaires métier (ex. traitement de déclarations)
- **xo** — lint (script `test`)

## Monitoring BullMQ (optionnel)

Le dashboard BullBoard est disponible sur `/admin/queues` si `BULLBOARD_PASSWORD` est renseigne.

## Prérequis

- **Redis** accessible (obligatoire au démarrage). En local : `docker compose up -d` — Redis écoute sur le port **6380** (mappé depuis 6379 dans le conteneur), cohérent avec `REDIS_URL` dans `.env.example`.

## Installation

```bash
npm install
```

## Configuration

Copier `.env.example` vers `.env` et renseigner les variables.

| Variable | Rôle |
|----------|------|
| `PORT` | Port HTTP du serveur (défaut : `4000`) |
| `REDIS_URL` | URL Redis pour BullMQ (ex. `redis://localhost:6380`) |
| `REDIS_TLS_CA_FILE_PATH` | CA pour Redis TLS si besoin |
| `PLE_BASE_URL` | URL de base de l’API PLE |
| `CLIENT_ID` / `CLIENT_SECRET` | Identifiants OAuth du compte service PLE |
| `PLE_WEBHOOK_SECRET` | Secret HMAC pour valider `X-PLE-Signature` sur `/hooks/declarations` |
| `WILLIE_API_TOKEN` | Bearer pour l’API Willie |
| `ORANGE_LIVE_OBJECTS_API_KEY` | Clé API Orange Live Objects |
| `SENTRY_DSN` / `SENTRY_ENV` | Télémétrie Sentry (optionnel) |

**Mode PLE** : si `PLE_BASE_URL`, `CLIENT_ID` et `CLIENT_SECRET` sont tous renseignés, le client appelle l’API réelle (tokens, déclarants, contextes, `ingest`). Sinon, les réponses sont tirées de `mock_responses.ts` et l’ingestion ne fait qu’un log (pas d’appel HTTP).
- `WILLIE_API_TOKEN`: token Bearer pour l'API Willie
- `ORANGE_LIVE_OBJECTS_API_KEY`: cle API pour l'API Orange Live Objects
- `PLE_BASE_URL`: URL de base de l'API Partageons l'eau (optionnel tant que le mode mock est actif)
- `CLIENT_ID`: identifiant client pour generer le JWT service account
- `CLIENT_SECRET`: secret client pour generer le JWT service account
- `BULLBOARD_PASSWORD`: mot de passe pour activer BullBoard (dashboard BullMQ)

## Scripts

- `npm run dev` — serveur en TypeScript avec chargement de `.env` (`node --env-file=.env --import tsx index.ts`)
- `npm run build` — compilation vers `dist/`
- `npm run start` — exécution de `dist/index.js` (nécessite un build préalable)
- `npm run check` — `tsc --noEmit`
- `npm run lint` / `npm run lint:fix` — xo
- `npm test` — alias sur le lint xo

## API HTTP

| Méthode | Chemin | Description |
|---------|--------|-------------|
| `GET` | `/health` | Santé du service (`{ ok: true }`) |
| `POST` | `/jobs/pull-updated-data` | Enfile un job `pull-updated-data` (réponse `202` + `jobId`) |
| `POST` | `/hooks/declarations` | Webhook PLE : corps `{ "event": "declaration.uploaded", "declarationId": "..." }`, en-tête `X-PLE-Signature` (HMAC-SHA256 hex du corps brut, secret `PLE_WEBHOOK_SECRET`) |
| `GET` | `/debug-sentry` | Déclenche une erreur de test pour Sentry |

## Files et planification

- **`pull-updated-data`** : planifié chaque jour à **03:00** (cron `0 0 3 * * *` côté BullMQ). Peut aussi être déclenché via `POST /jobs/pull-updated-data`.
- **`process-declaration`** : enfilement depuis le webhook déclarations (idempotence par `jobId` dérivé de `declarationId`).

Les workers tournent dans le même processus que le serveur HTTP (concurrence **1** par file).

## Architecture (fichiers)

- `index.ts` — importe et démarre `src/server.ts`
- `src/server.ts` — Express, routes, graceful shutdown (workers, queues, Redis, Sentry)
- `src/instrument.ts` — initialisation Sentry
- `src/queues/config.ts` — définition des jobs et files BullMQ
- `src/queues/redis.ts` — connexion Redis
- `src/queues/jobs.ts` — `addJobPullUpdatedData`, `addJobProcessDeclaration`
- `src/queues/scheduler.ts` — planification cron BullMQ
- `src/queues/workers.ts` — workers `pull-updated-data` / `process-declaration`
- `src/jobs/pull_updated_data.ts` — orchestration du pull
- `src/jobs/process-declaration.ts` — traitement d’une déclaration uploadée
- `src/connectors/` — `base-connector.ts`, `types.ts`, implémentations enregistrées dans `index.ts`
- `src/services/partageonsleau-client.ts` — client PLE (mock ou API)
- `src/services/mock_responses.ts` — données locales lorsque l’API PLE n’est pas configurée

### Connecteurs enregistrés

- `willie`
- `orange_live_objects`
- `aquasys`
- `template_file`

## Contrat de sortie connecteur

Chaque connecteur produit un payload standardisé par point :

- `id_point_de_prelevement`
- `metrics[]` avec :
  - `type` (`index` ou `volume_preleve` — enum `MetricType` dans `types.ts`)
  - `frequency` / granularité
  - `values[]` (`date`, `value`)
  - `unit` (ex. `m3` ou `null`)

## Willie (comportement actuel)

Le connecteur Willie appelle :

- `GET https://api.meetwillie.com/v1/stations/consumption`

Paramètres typiques :

- `stationIds` = `sourcePointId` (identifiant station Willie)
- `startDate` = `lastRunAt` (ou repli si absent)
- `endDate` = maintenant
- `resolution` = `day`

La réponse `stations[].datapoints[]` est mappée vers le format commun.

## Pistes d’évolution

- Réduire la dépendance aux mocks (`mock_responses`) en environnements de dev
- Rendre certaines options (fréquences, cron) configurables par variables d’environnement
- Ajouter des tests ciblés par connecteur et par job
