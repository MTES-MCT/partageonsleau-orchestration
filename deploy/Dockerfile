FROM node:24-bookworm-slim AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY index.ts ./
COPY src ./src

RUN npm run build
RUN npm prune --omit=dev

FROM node:24-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV NODE_OPTIONS=--use-openssl-ca
ENV PORT=4000

RUN apt-get update \
  && apt-get install -y --no-install-recommends dumb-init ca-certificates \
  && rm -rf /var/lib/apt/lists/*


COPY package*.json ./
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
