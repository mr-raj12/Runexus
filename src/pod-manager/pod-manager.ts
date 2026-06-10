import type { QueuedStep } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"

/**
 * The pod manager runs a single step inside a leased pod and reports its
 * lifecycle to the result queue. It never reads or writes workflow state.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    const pod = await podPool.acquirePod()
    const { podId } = pod

    await resultQueue.push({
      workflowId: step.workflowId,
      stepId: step.stepId,
      podId,
      status: "RUNNING",
    })

    try {
      const stdout = await podPool.execInPod(podId, step.command)
      await resultQueue.push({
        workflowId: step.workflowId,
        stepId: step.stepId,
        podId,
        status: "COMPLETED",
        stdout,
        exitCode: 0,
      })
    } catch (err) {
      await resultQueue.push({
        workflowId: step.workflowId,
        stepId: step.stepId,
        podId,
        status: "FAILED",
        error: err instanceof Error ? err.message : String(err),
      })
    } finally {
      await podPool.releasePod(podId)
    }
  }
}

export const podManager = new PodManager()
