# partageonsleau-orchestration

Orchestrateur TypeScript pour récupérer des données métier (connecteurs externes), les normaliser, puis les envoyer vers la plateforme Partageons l'eau (PLE). Le service expose une API HTTP, planifie des jobs et traite les webhooks PLE via **Redis** et **BullMQ**.

## Objectif du job « pull »

Le job `pull-updated-data` enchaîne :

1. sélection du compte de service configuré par `CLIENT_ID` (ou des comptes fictifs en mode local) ;
2. obtention d'un jeton opaque de compte de service auprès de l'API ;
3. récupération de la liste des déclarants renvoyée par l'API ;
4. pour chaque déclarant, récupération des contextes et points avec ce même jeton ;
5. exécution du connecteur associé à chaque point ;
6. normalisation du payload et envoi vers PLE (`ingest`) lorsque l’API est configurée.

Un second job, `process-declaration`, traite les déclarations déposées sur PLE (fichiers, points associés) après réception d’un webhook signé.

## Stack

- **Node.js** (version dans [`.nvmrc`](.nvmrc)) + **TypeScript** (ESM, `NodeNext`)
- **Express** 5 — API HTTP et webhooks
- **BullMQ** + **ioredis** — files d’attente et workers
- **Sentry** (`@sentry/node`, profiling) — erreurs et traces (optionnel via `SENTRY_DSN`)
- **moment**, **xlsx** — utilitaires métier (ex. traitement de déclarations)
- **ESLint** + **Prettier** — analyse et formatage (script `lint`)

## Monitoring BullMQ (optionnel)

Le dashboard BullBoard est disponible sur `/admin/queues` si `BULLBOARD_PASSWORD` est renseigné.

## Prérequis

- **Redis** accessible (obligatoire au démarrage). En local : `docker compose up -d` — Redis écoute sur le port **6380** (mappé depuis 6379 dans le conteneur), cohérent avec `REDIS_URL` dans `.env.example`.

## Installation

Utiliser les versions définies dans `.nvmrc` et dans le champ `packageManager`
de [`package.json`](package.json).

```bash
nvm use
npm ci
```

Ne pas utiliser `--force` ni `--legacy-peer-deps`. Réexaminer les scripts
d'installation autorisés dans `allowScripts` lors des mises à jour.

## Configuration

Copier `.env.example` vers `.env` et renseigner les variables.

| Variable | Rôle |
|----------|------|
| `PORT` | Port HTTP du serveur (défaut : `4000`) |
| `REDIS_URL` | URL Redis pour BullMQ (ex. `redis://localhost:6380`) |
| `REDIS_TLS_CA_FILE_PATH` | CA pour Redis TLS si besoin |
| `PLE_BASE_URL` | URL de base de l’API PLE |
| `CLIENT_ID` / `CLIENT_SECRET` | Identifiants du compte de service PLE, échangés contre un jeton opaque |
| `PLE_WEBHOOK_SECRET` | Secret HMAC pour valider `X-PLE-Signature` sur `/hooks/declarations` |
| `WILLIE_API_TOKEN` | Bearer pour l’API Willie |
| `ORANGE_LIVE_OBJECTS_API_KEY` | Clé API Orange Live Objects |
| `SENTRY_DSN` / `SENTRY_ENV` | Télémétrie Sentry (optionnel) |
| `BULLBOARD_PASSWORD` | Mot de passe pour activer BullBoard (dashboard BullMQ) |

**Mode PLE** : si `PLE_BASE_URL`, `CLIENT_ID` et `CLIENT_SECRET` sont tous renseignés, le client appelle l’API réelle (tokens, déclarants, contextes, `ingest`). Sinon, les réponses sont tirées de `mock_responses.ts` et l’ingestion ne fait qu’un log (pas d’appel HTTP).

**CA Redis des images déployées** : le Dockerfile charge `deploy/certs/${ENV_NAME}/redis-ca.pem` et refuse de construire l’image si ce fichier est absent ou vide. Ce certificat doit provenir de l’instance Redis de l’environnement ciblé, jamais d’un autre environnement. Dans le conteneur, `REDIS_TLS_CA_FILE_PATH` doit pointer vers `/usr/local/share/ca-certificates/scw-redis-ca.crt`.

## Scripts

- `npm run dev` — serveur TypeScript auto-reloadable avec chargement de `.env` (`node --env-file=.env --watch --import tsx index.ts`)
- `npm run build` — compilation vers `dist/`
- `npm run start` — exécution de `dist/index.js` (nécessite un build préalable)
- `npm run check` — `tsc --noEmit`
- `npm run lint` / `npm run lint:fix` — ESLint et Prettier
- `npm test` — tests unitaires avec le lanceur de tests Node.js et `tsx`
- `npm run replay:declaration -- <code-ou-uuid> --env-file .env` — relance directement le traitement d'une déclaration sur l'API configurée ; vérifier la cible avant exécution, ce traitement écrit des données

## Vérifications et maintenance

```bash
npm audit --include=dev --audit-level=low
npm audit --omit=dev --audit-level=low
npm run check
npm run lint
npm test
npm run build
```

Les imports sont testés sur des fixtures CSV/XLS/XLSX, sans connecteur réel.
Les tests de compatibilité BullMQ 5/6 nécessitent `QUEUE_INTEGRATION_TESTS=1`
et un `REDIS_URL` vers un Redis local jetable, base 1 ou 2. Chaque essai utilise
un préfixe aléatoire. L'alias `bullmq-v5` est réservé aux tests et absent de
l'image finale ; ioredis utilise RESP2.

Ces tests ne remplacent pas la recette des connecteurs externes. Pour les contrôles
CI et le déploiement, voir le
[guide commun des pipelines](https://github.com/MTES-MCT/prelevements-deau-api/blob/testing/docs/pipelines.md).

## API HTTP

| Méthode | Chemin | Description |
|---------|--------|-------------|
| `GET` | `/health` | Santé du service (`{ ok: true }`) |
| `POST` | `/hooks/declarations` | Webhook PLE : corps `{ "event": "declaration.uploaded", "declarationId": "..." }`, en-tête `X-PLE-Signature` (HMAC-SHA256 hex du corps brut, secret `PLE_WEBHOOK_SECRET`) |

## Files et planification

- **`pull-updated-data`** : planifié chaque jour à **03:00** (cron `0 0 3 * * *` côté BullMQ, sans fuseau explicite). Aucun déclenchement HTTP public ; le scheduler enfile directement le job. Le traitement nécessite un worker actif.
- **`process-declaration`** : enfilement depuis le webhook déclarations (idempotence par `jobId` dérivé de `declarationId`).

Les workers tournent dans le même processus que le serveur HTTP (concurrence **1** par file).

### Rives et Eaux (Calypso)

Le job dédié `pull-rives-et-eaux` est désactivé par défaut. Il s'active avec
`RIVES_ET_EAUX_ENABLED=true`, `RIVES_ET_EAUX_API_KEY` en variable secrète et les
identifiants PLE habituels. `RIVES_ET_EAUX_BASE_URL` vaut par défaut
`https://services.riveseteaux.fr` ; `RIVES_ET_EAUX_TIMEZONE` doit être `Europe/Paris`.
La clé fournisseur ne doit figurer ni dans les paramètres des exploitations ni dans Git.

Chaque jour à **03:30 Europe/Paris**, le job relit les **15 jours locaux révolus**,
un appel `/api/public/Calypso/Export` par journée pour tous les compteurs.
`dateDebut` et `dateFin` portent les dates locales des deux minuits consécutifs.
Les doublons identiques aux bornes inclusives sont éliminés ; les observations
contradictoires et lignes malformées sont conservées. Le connecteur traduit seul
le format Calypso vers des relevés génériques : `externalId`, `observedAt`, `index`
décimal, `status`, `reason`, `quality`, `origin` et preuve originale `raw`.
Les codes A/B/C/D/E/X donnent un relevé admissible ; Y/W/Z et les codes inconnus
donnent `status: INVALID`. Les timestamps locaux originaux restent dans `raw`,
avec leur conversion UTC exacte ; les heures ambiguës ou inexistantes donnent
`observedAt: null` et sont retransmises dans chaque fenêtre concernée.
Aucune répartition, arrondi au quart d'heure ou reconstruction de volume n'est faite ici.

Le client générique `MeterReadingsClient` lit
`GET /service-accounts/meter-streams?provider=rives-et-eaux&scope=epidropt`.
Sans flux autorisé, aucun appel fournisseur n'est effectué. Les lots sont envoyés à
`POST /service-accounts/meter-readings/ingestions`, avec `provider`, `scope` et le
mode `LIVE` ; seul un
accusé `persisted: true`, avec le nombre reçu et le checkpoint de la fenêtre,
permet de passer à la journée suivante. L'API conserve le checkpoint durable,
filtre les compteurs autorisés et décide de la publication selon les affectations,
statuts normalisés et conflits. L'API ne connaît ni Calypso, ni ses codes qualité,
ni son fuseau horaire. Les dates antérieures à l'activation sont conservées à l'état brut.

Un échec fournisseur ou PLE fait échouer le job (trois tentatives BullMQ, délai
exponentiel). Les requêtes natives `fetch` conservent la configuration du proxy,
ont un timeout et refusent les redirections ; les erreurs n'exposent ni clé ni
corps des réponses. Le nouveau chemin ne passe pas par le connecteur générique
par PP et n'applique pas `connector.rate`.

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

Le [registre](src/connectors/index.ts) comprend :

- API : `willie`, `orange_live_objects`, `omniscient_murgat`.
- Fichiers : `aquasys`, `bv_tech`, `template_file`, `smnpr`, `gidaf`.

## Contrat de sortie connecteur

Chaque connecteur produit un payload standardisé par point, défini dans
[`src/connectors/types.ts`](src/connectors/types.ts) :

- `id_point_de_prelevement`
- `flow_type` éventuel (`PRELEVEMENT` ou `REJET`), `source_type` et métadonnées de source
- `metrics[]` avec :
  - `type` (`index`, `volume` ou `debit`)
  - `granularity`, `conflictPolicy` et `usage` éventuel
  - `values[]` (`date`, `value`, éventuellement `periodStart` et `periodEnd` pour une période semi-ouverte)
  - `unit` (`m3`, `L/s` ou non renseignée)

## Willie (comportement actuel)

Le connecteur Willie appelle :

- `GET https://api.meetwillie.com/v1/stations/consumption`

Paramètres typiques :

- `stationIds` = `sourcePointId` (identifiant station Willie)
- `startDate` = `mostRecentAvailableDate` renvoyée par l'API, ou date d'activation définie dans le connecteur si aucune donnée n'est disponible
- `endDate` = maintenant
- `resolution` = `day`

La réponse `stations[].datapoints[]` est mappée vers le format commun.
