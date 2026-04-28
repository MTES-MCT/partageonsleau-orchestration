FROM node:24.15.0-bookworm-slim AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY index.ts ./
COPY src ./src

RUN npm run build


FROM node:24.15.0-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV NODE_OPTIONS=--use-openssl-ca
ENV PORT=3000

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

RUN apt-get update \
  && apt-get install -y --no-install-recommends dumb-init ca-certificates \
  && rm -rf /var/lib/apt/lists/*

ARG ENV_NAME=testing

COPY deploy/certs/${ENV_NAME}/redis-ca.pem /usr/local/share/ca-certificates/scw-redis-ca.crt
RUN update-ca-certificates

EXPOSE 3000

ENTRYPOINT ["dumb-init", "--"]

CMD ["node", "dist/index.js"]
