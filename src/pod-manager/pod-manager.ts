import type { QueuedStep, StepResultStatus } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { resultQueue } from "../queue/result-queue"

const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS) || 10_000

/**
 * The pod manager runs a single step inside a leased pod and reports its
 * lifecycle to the result queue. It never reads or writes workflow state.
 *
 * While the command runs it emits a heartbeat on a fixed interval, each one
 * confirming the pod is still alive. A consumer that sees the heartbeats stop
 * knows the pod died rather than the command simply taking a long time.
 */
export class PodManager {
  async dispatch(step: QueuedStep): Promise<void> {
    const pod = await podPool.acquirePod()
    const { podId } = pod

    await this.report(step, podId, "RUNNING")
    const stopHeartbeat = this.startHeartbeat(step, podId)

    try {
      const stdout = await podPool.execInPod(podId, step.command)
      await this.report(step, podId, "COMPLETED", { stdout, exitCode: 0 })
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err)
      await this.report(step, podId, "FAILED", { error })
    } finally {
      stopHeartbeat()
      await podPool.releasePod(podId)
    }
  }

  private startHeartbeat(step: QueuedStep, podId: string): () => void {
    let running = true

    const beat = async (): Promise<void> => {
      if (!running) return
      const alive = await podPool.isPodAlive(podId)
      if (!running) return

      if (alive) {
        await this.report(step, podId, "HEARTBEAT")
      } else {
        // The pod vanished mid-step. Stop beating and let the failed exec (or
        // the consumer's liveness timeout) drive the step to a terminal state.
        running = false
        return
      }

      if (running) timer = setTimeout(beat, HEARTBEAT_INTERVAL_MS)
    }

    let timer = setTimeout(beat, HEARTBEAT_INTERVAL_MS)

    return () => {
      running = false
      clearTimeout(timer)
    }
  }

  private async report(
    step: QueuedStep,
    podId: string,
    status: StepResultStatus,
    extra: { stdout?: string; exitCode?: number; error?: string } = {}
  ): Promise<void> {
    await resultQueue.push({
      workflowId: step.workflowId,
      stepId: step.stepId,
      podId,
      status,
      at: Date.now(),
      ...extra,
    })
  }
}

export const podManager = new PodManager()
