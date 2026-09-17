FROM node:24.21.0-alpine3.24@sha256:be80f76cf40ec8e42b9bec49f60a55e0660f30af58d3e5a25530785b30ea67e2 AS base
RUN apk upgrade --no-cache
WORKDIR /app
COPY package.json ./
RUN npm install --global "$(node -p 'require("./package.json").packageManager')" --ignore-scripts --no-audit --no-fund

FROM base AS builder

WORKDIR /app

COPY package.json package-lock.json .npmrc ./
RUN npm ci

COPY tsconfig.json ./
COPY index.ts ./
COPY src ./src

RUN npm run build
RUN npm prune --omit=dev

FROM base AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NODE_OPTIONS=--use-openssl-ca
ENV PORT=4000

RUN apk add --no-cache dumb-init ca-certificates


COPY package.json package-lock.json .npmrc ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

ARG ENV_NAME=testing

# A managed Redis CA must belong to the target instance. Never fall back to a
# certificate copied from another environment.
COPY deploy/certs /tmp/redis-certs
RUN set -eu; \
  redis_ca="/tmp/redis-certs/${ENV_NAME}/redis-ca.pem"; \
  test -s "$redis_ca"; \
  install -m 0644 "$redis_ca" /usr/local/share/ca-certificates/scw-redis-ca.crt; \
  update-ca-certificates; \
  rm -rf /tmp/redis-certs

EXPOSE 4000

ENTRYPOINT ["dumb-init", "--"]

CMD ["node", "dist/index.js"]
