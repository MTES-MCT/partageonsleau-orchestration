# Maintenance sécurité et CI

## Installer et vérifier

La version de Node est dans `.nvmrc`, celle de npm dans `packageManager`.
Les fichiers internes de npm ne sont jamais modifiés. La version officielle 11.19.1
intègre les correctifs nécessaires ([notes officielles](https://github.com/npm/cli/releases/tag/v11.19.1)).

```sh
nvm use
npm install --global "$(node -p 'require("./package.json").packageManager')" --ignore-scripts --no-audit --no-fund
npm ci
npm audit --include=dev --audit-level=low
npm audit --omit=dev --audit-level=low
bash .github/scripts/check-workflows.sh
```

Utiliser `npm ci`, sans `--force` ni `--legacy-peer-deps`. Les scripts des dépendances
sont autorisés explicitement dans `allowScripts` ; réexaminer ces autorisations à chaque
mise à jour. ShellCheck doit être installé pour valider les workflows.

## Pipelines et déploiement

- Les PR vers testing/demo/prod et les déploiements appellent `quality.yml`.
  Audit, lint et tests ne sont pas recopiés dans les étapes de déploiement.
- Les commandes natives npm et Trivy bloquent sur toute vulnérabilité signalée,
  même sans correctif. Trivy bloque également un OS en fin de support.
  Les erreurs des outils échouent aussi ; aucune liste d'exclusion.
- L'image est scannée par digest avant migration/déploiement. Le digest testé
  est celui déployé, l'alias d'environnement est publié après vérification.
- Les contrôles de cible et de conservation des réglages restent obligatoires :
  pas de remplacement des variables ou secrets par une liste partielle.
- Les audits sont conservés dans les artefacts CI, hors Git.
  Les tests utilisent uniquement des données synthétiques et services jetables.

## Avant la première promotion

- [ ] Corriger les alertes bloquantes et valider les trois projets sur les commits exacts.
- [ ] Préparer sauvegardes et bascule coordonnée API/worker/orchestrateur :
  voir `prelevements-deau-api/docs/bullmq-6-migration.md`. Ne pas purger Redis.
- [ ] Recetter testing : connexion, rôles, déclarations/campagnes, compteurs,
  cartes, fichiers Excel/S3, mails et reprise des tâches.
- [ ] Promouvoir demo puis prod uniquement après décision explicite.

Les contrôles locaux ne valent ni exécution des pipelines GitHub ni recette des
services externes réels. Les branches main, demo et prod ne sont pas modifiées par ce travail.

## Contrôles orchestrateur

```sh
node --test .github/scripts/deployment-policy.test.mjs
npm run check
npm run lint
npm test
npm run build
```

Un seul compilateur TypeScript 6.0.3 est utilisé pour le contrôle et le build.
ESLint utilise les règles recommandées typées et Prettier conserve le format
du dépôt. L'exception `require-await` est limitée aux contrats asynchrones
des connecteurs/clients et aux fixtures de files, pas à tout le code.

Les imports sont vérifiés sur des fixtures CSV/XLS/XLSX, sans connecteur réel.
`QUEUE_INTEGRATION_TESTS=1` et une URL Redis locale vers la base 1 ou 2
activent les tests BullMQ 5/6. Chaque essai utilise un préfixe aléatoire.
L'alias `bullmq-v5` est une dépendance de test uniquement ; il n'est pas
embarqué dans l'image finale. Le client ioredis reste explicitement en RESP2.

L'image démarre directement avec Node et dumb-init ; sa sonde est `/health`.
Les cibles sont centralisées dans `.github/scripts/deploy-container.mjs`.
