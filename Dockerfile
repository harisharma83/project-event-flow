# One image, seven services. Every EventFlow process (Order Service,
# Poller, Inventory Service, Payment Service, Saga Orchestrator, Read
# Model Service, Notification Service) lives in this same package.json,
# shares the same node_modules, and has no build step (no TypeScript, no
# bundler) — so seven near-identical Dockerfiles would just be seven
# copies of the same 6 lines. docker-compose.yml builds this ONE image
# once (tagged eventflow-app:local) and every service picks its own
# entry point via `command:`, not via a separate image.
#
# Single-stage, deliberately — multi-stage buys you a smaller image by
# separating "things needed to build" from "things needed to run," but
# this project has nothing to build. The moment a real build step shows
# up (TypeScript, a bundler), this is where a second FROM would go: a
# builder stage that runs it, and this final stage copying only the
# compiled output.
FROM node:20-slim

WORKDIR /app

# Copy manifests first, install, THEN copy source — so Docker's layer
# cache only re-runs npm ci when package.json/package-lock.json actually
# change, not on every source edit.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

# No default CMD — every service in docker-compose.yml supplies its own
# `command:` (node src/server.js, node src/poller.js, etc.) against this
# one image.
