import type { StepResult } from "../types/workflow"
import { redis } from "./redis-client"

const QUEUE_NAMESPACE = process.env.REDIS_QUEUE_PREFIX || `workflow:${process.pid}`
const RESULT_QUEUE_KEY = `${QUEUE_NAMESPACE}:result-queue`

function parseStepResult(value: string): StepResult {
  return JSON.parse(value) as StepResult
}

/**
 * Redis-backed queue for the lifecycle events the pod manager publishes.
 * The blocking consumer uses a dedicated connection so it never starves the
 * shared client used for pushes.
 */
export class ResultQueue {
  async push(result: StepResult): Promise<void> {
    await redis.lpush(RESULT_QUEUE_KEY, JSON.stringify(result))
  }

  async consume(handler: (result: StepResult) => Promise<void>): Promise<void> {
    const subscriber = redis.duplicate()

    while (true) {
      const item = await subscriber.brpop(RESULT_QUEUE_KEY, 0)
      if (!item) continue

      const [, value] = item
      await handler(parseStepResult(value))
    }
  }
}

export const resultQueue = new ResultQueue()
