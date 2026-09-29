# Guide des agents — orchestrateur Partageons l’eau

## Rôle et limites

- Dépôt indépendant de l’API et du front : vérifier la branche et le diff avant toute modification.
- Service TypeScript ESM, Node.js, Express, BullMQ et Redis ; compilation vers `dist/`.
- Il récupère les données fournisseurs, analyse les fichiers de déclaration et transmet des données normalisées à l’API.
- L’API reste responsable des autorisations, de la persistance métier et des calculs de volumes des compteurs physiques.
- Le front passe par l’API ; ne pas créer de dépendance directe du navigateur à l’orchestrateur.
- Ne pas déplacer les parseurs de fichiers vers l’API ni créer de routes métier spécifiques à un fournisseur dans celle-ci.

## Repères

- `index.ts` démarre `src/server.ts` : HTTP, signature des webhooks et arrêt propre.
- `src/connectors/` contient les parseurs et connecteurs ; `index.ts` y définit le registre et `types.ts` le contrat commun.
- `src/jobs/` orchestre la récupération, les déclarations et la collecte des compteurs.
- `src/services/partageonsleau-client.ts` porte le transport des connecteurs vers l’API.
- `src/services/meter-readings-client.ts` porte le contrat générique des relevés de compteurs.
- `src/queues/` contient connexions Redis, files, workers et planification ; les workers tournent avec le serveur HTTP.
- Les tests `*.test.ts` sont voisins du code ; les fixtures de parseurs sont dans `src/connectors/fixtures/`.
- Suivre TypeScript strict, les imports ESM avec extension `.js` et le format ESLint/Prettier existant.

## Installation et commandes

Utiliser Node selon `.nvmrc` et npm selon `packageManager` dans `package.json`.
Installer avec `nvm use`, puis `npm ci` ; ne pas contourner les dépendances avec `--force` ou `--legacy-peer-deps`.
Le démarrage exige Redis et une configuration locale préparée depuis `.env.example`, sans publier ni versionner les secrets.
`docker compose up -d redis` démarre le Redis local décrit par `compose.yaml`.

| Commande | Usage |
| --- | --- |
| `npm run dev` | Démarrage TypeScript avec surveillance et chargement de `.env`. |
| `npm run check` | Vérification TypeScript sans émission. |
| `npm run lint` | ESLint et vérification Prettier, sans réécriture. |
| `npm test` | Tests Node.js avec `tsx`, sans isolation des fichiers de test. |
| `npm run build` | Compilation TypeScript dans `dist/`. |
| `npm start` | Exécution de `dist/index.js`, après compilation ; environnement fourni au processus. |
| `npm run replay:declaration -- <code-ou-uuid> --env-file .env` | Rejeu avec écriture sur l’API configurée : cible et autorisation à vérifier avant exécution. |

Le démarrage installe les schedulers et active les workers : ce n’est pas un diagnostic neutre.
Le mode fictif de `PartageonsLeauClient` désactive les appels PLE si sa configuration est incomplète ; il ne garantit pas l’absence d’appels fournisseurs.
Ne pas utiliser un Redis partagé ou un environnement distant pour des essais locaux.

## Contrats et données

- Préserver les champs du contrat commun : identifiant du point, type de flux, source, métriques, unités, usages, granularité et politique de conflit.
- Ne pas confondre `pointId` interne, `sourcePointId` fournisseur et `connectorId` ; préserver les identifiants existants lors d’un rejeu.
- Les périodes explicites sont semi-ouvertes `[periodStart, periodEnd)` ; ne pas remplacer une période par une date ponctuelle.
- Respecter les conventions de dates propres aux fichiers et fournisseurs ; tester les frontières de jours, mois et changements d’heure concernés.
- Réutiliser les clients existants et coordonner tout changement de payload avec l’API ; vérifier les écrans affectés côté front si le contrat utilisateur change.
- Les comptes de service obtiennent leur jeton auprès de l’API ; ne pas supposer son format ni décoder un JWT pour décider des droits.
- Les fichiers des déclarations signalées via `/hooks/declarations` sont traités par `process-declaration` ; conserver leurs références de source et les erreurs de parsing utiles.
- Un nouveau connecteur classique s’enregistre dans le registre ; le chemin des compteurs physiques conserve son transport générique distinct.

## Reprise, index et volumes

- Le pull classique repart de `mostRecentAvailableDate` ou de la date d’activation du connecteur ; ne pas élargir silencieusement une fenêtre historique.
- Les jobs sont retentés avec délai exponentiel ; conserver les identifiants de jobs/schedulers et vérifier les effets d’un traitement répété.
- Le `jobId` des déclarations limite les doublons en file, mais ne garantit pas à lui seul une ingestion exactement une fois après suppression d’un job terminé.
- Le pull classique journalise les erreurs par point et continue : un job terminé ne prouve pas que tous les points ont été importés.
- La collecte Rives relit des fenêtres journalières et transmet des relevés génériques avec `provider`, `scope`, `externalId`, index décimal et preuve `raw`.
- Elle conserve les contradictions et données invalides utiles au diagnostic ; ne pas transformer une absence ou un index invalide en zéro.
- Les lots compteurs ne sont acquittés qu’après `persisted: true`, nombre reçu correct et checkpoint correspondant à la fin de fenêtre.
- L’API conserve le checkpoint durable, déduplique les observations et applique les affectations ; ne pas calculer ici un second volume depuis les mêmes index.
- Ne pas appliquer `connector.rate` au chemin des compteurs physiques ni inventer des pourcentages, rattachements ou dates d’effet historiques.
- Un rattrapage historique ou une modification de politique de remplacement exige un périmètre explicite et une vérification des volumes déjà présents.

## Réseau, authentification et secrets

- Utiliser les appels `fetch` natifs et les clients existants ; la prise en charge du proxy d’environnement est testée dans `src/network/env-proxy.test.ts`.
- Préserver `NODE_USE_ENV_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY` et `NO_PROXY` ; ne pas imposer d’agent HTTP contournant cette configuration.
- Pour un nouvel appel externe, prévoir un délai borné et contrôler les redirections, notamment lorsqu’un secret est transmis.
- Une nouvelle destination peut nécessiter une autorisation du proxy : signaler ce besoin, sans contourner le filtrage ni élargir `NO_PROXY` automatiquement.
- Préserver la vérification HMAC de `X-PLE-Signature` sur le corps brut et l’authentification de BullBoard ; ne pas ajouter de déclencheur public de synchronisation.
- Garder les secrets dans les variables d’environnement prévues, jamais dans les paramètres métier, fixtures, commandes journalisées ou commits.
- Ne pas consigner les jetons, URL signées ou réponses fournisseur complètes dans les erreurs/Sentry ; privilégier statuts et comptes agrégés.
- Préserver la validation TLS et la CA Redis de l’environnement ; ne jamais désactiver la vérification des certificats.

## Vérification et livraison

- Documentation seule : relire les chemins/commandes et exécuter `git diff --check` ; pas de build local, import ou appel fournisseur nécessaire.
- Code : tests ciblés, puis `npm run check`, `npm run lint`, `npm test` et `npm run build` selon l’étendue ; préciser toute vérification non exécutée.
- Parseur/connecteur : fixtures synthétiques, identifiants, unités, dates, doublons, erreurs et payload envoyé à l’API ; pas de recette sur données réelles sans accord.
- Compteurs : couvrir index invalides, fenêtres chevauchantes, rejeu et échec d’acquittement ; vérifier qu’aucun volume n’est calculé dans ce transport.
- Files/Redis : activer les tests de compatibilité uniquement avec `QUEUE_INTEGRATION_TESTS=1` et `REDIS_URL` vers un Redis local jetable, base 1 ou 2.
- Dépendances : vérifier aussi les audits développement et production (`npm audit --include=dev --audit-level=low` et `npm audit --omit=dev --audit-level=low`).
- Ne pas lancer de replay, synchronisation, commit, push ou déploiement au-delà de la demande explicite.
- Si un déploiement est demandé, utiliser les workflows GitHub Actions existants de `testing`, `demo` ou `prod`, sans modifier les configurations des autres environnements.
- Vérifier le résultat du workflow ciblé et `/health` ; ne pas assimiler un push réussi à un déploiement réussi.
- Ne pas ajouter au dépôt public de comptes rendus d’incident, inventaires d’infrastructure, accès privés ou scripts ponctuels sans nécessité durable.
