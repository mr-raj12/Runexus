import { orchestrator } from "./backend/orchestrator"
import { app } from "./backend/server"
import { podPool } from "./k8s/pod-pool"
import { redis } from "./queue/redis-client"

async function main(): Promise<void> {
  await redis.ping()
  console.log("Redis connected")

  await podPool.ready
  const pool = podPool.getPoolStatus()
  console.log(`Pod pool ready: ${pool.total} runners`)

  orchestrator.start()

  const port = Number(process.env.PORT) || 3000
  app.listen(port, () => console.log(`Server listening on port ${port}`))
}

main().catch((err: unknown) => {
  console.error("Startup failed:", err)
  process.exit(1)
})
