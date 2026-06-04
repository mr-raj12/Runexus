import type { QueuedStep } from "../types/workflow"
import { redis } from "./redis-client"

const QUEUE_NAMESPACE = process.env.REDIS_QUEUE_PREFIX || `workflow:${process.pid}`
const STEP_QUEUE_KEY = `${QUEUE_NAMESPACE}:step-queue`

function parseQueuedStep(value: string): QueuedStep {
  return JSON.parse(value) as QueuedStep
}

/**
 * FIFO queue of steps awaiting a runner pod. Enqueue is LPUSH, dequeue is a
 * blocking BRPOP on a dedicated connection so the drain loop parks instead of
 * busy-waiting when the queue is empty.
 */
export class StepQueue {
  private blockingClient = redis.duplicate()

  async enqueue(step: QueuedStep): Promise<void> {
    await redis.lpush(STEP_QUEUE_KEY, JSON.stringify(step))
  }

  async dequeue(): Promise<QueuedStep | null> {
    const value = await this.blockingClient.brpop(STEP_QUEUE_KEY, 0)
    if (!value) return null
    const [, stepValue] = value
    return parseQueuedStep(stepValue)
  }

  async size(): Promise<number> {
    return redis.llen(STEP_QUEUE_KEY)
  }
}

export const stepQueue = new StepQueue()
