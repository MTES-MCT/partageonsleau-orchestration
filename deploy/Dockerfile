FROM node:24.15.0-bookworm-slim AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY index.ts ./
COPY src ./src

RUN npm run build
RUN npm prune --omit=dev

FROM node:24.15.0-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV NODE_OPTIONS=--use-openssl-ca
ENV PORT=3000

RUN apt-get update \
  && apt-get install -y --no-install-recommends dumb-init ca-certificates \
  && rm -rf /var/lib/apt/lists/*


COPY package*.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist

ARG ENV_NAME=testing

COPY deploy/certs/${ENV_NAME}/redis-ca.pem /usr/local/share/ca-certificates/scw-redis-ca.crt
RUN update-ca-certificates

EXPOSE 3000

ENTRYPOINT ["dumb-init", "--"]

CMD ["node", "dist/index.js"]
