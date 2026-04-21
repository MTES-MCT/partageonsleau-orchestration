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

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

RUN apt-get update \
  && apt-get install -y dumb-init \
  && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["dumb-init", "--"]

CMD ["node", "dist/index.js"]
