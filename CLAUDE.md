# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Sample Node.js service used as a LocalOps lab/demo project. It contains multiple service entry points demonstrating different workload types (web, worker, cron, job) that are containerized and deployed to Kubernetes.

## Commands

- **Run the main web service:** `npm start` (runs `node web.js` on port 3000)
- **Install dependencies:** `npm install`
- **Run other entry points:** `node worker.js`, `node job.js`, `node cron.js`, `node internal.js` (port 3001), `node another-webservice.js`
- **No test suite** — `npm test` is a placeholder that exits with an error

## Architecture

- **Node 22** (see `.nvmrc`) with Express 5
- Multiple independent entry points at the root, each representing a different workload type:
  - `web.js` / `another-webservice.js` / `web-sec.js` — HTTP services (port 3000) returning JSON or HTML
  - `internal.js` — internal HTTP service on port 3001
  - `worker.js` — long-running background process (interval loop)
  - `job.js` — one-shot job (runs then exits after timeout)
  - `cron.js` — scheduled cron job (runs then exits after timeout)
- `backend/` — separate service with its own `package.json`, `Dockerfile`, and Express app (port 3000)
- **Dockerfiles:** `Dockerfile` (ENTRYPOINT+CMD pattern), `Dockerfile.cmd` (CMD-only pattern), `Dockerfile.secret` (build-time secrets via `--mount=type=secret`)
- `ops.json.sample` — LocalOps configuration declaring infrastructure dependencies (S3, SNS, SQS, RDS, ElastiCache) and cron schedules
- `kube/` — Kubernetes manifests (deployment, cron job)

## Environment Variables

Services read these from the environment at runtime:
- `ENV` — environment name
- `DB_HOST` — database host
- `SECRET_KEY` — secret key (used in `web.js` and `web-sec.js`)
