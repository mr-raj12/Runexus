# Workflow Orchestrator

A DAG workflow orchestrator that runs shell-command steps across a pool of
Kubernetes runner pods. Steps are dispatched when their dependencies complete
and their results are streamed back through Redis.

Work in progress. See `package.json` for scripts.

## Stack

- Bun + TypeScript
- Express for the HTTP API
- Redis (ioredis) for the work queues
- Kubernetes (kind locally) for the runner pods

## Scripts

```bash
bun install        # install dependencies
bun run dev        # start the server with watch mode
bun run typecheck  # type-check the project
```
