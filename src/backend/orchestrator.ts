import type { QueuedStep, StepResult, StepStatus, Workflow, WorkflowState } from "../types/workflow"
import { getAllWorkflows, getWorkflow, setWorkflow } from "./workflow-store"
import { assertValidDag, getReadySteps, isTerminal, skipDependents } from "./dag"
import { stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"

const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS) || 10_000
const MISSED_HEARTBEATS_BEFORE_DEAD = Number(process.env.HEARTBEAT_GRACE_BEATS) || 3
const LIVENESS_TIMEOUT_MS = HEARTBEAT_INTERVAL_MS * MISSED_HEARTBEATS_BEFORE_DEAD
const LIVENESS_SCAN_INTERVAL_MS = Number(process.env.LIVENESS_SCAN_INTERVAL_MS) || 5_000

/**
 * The orchestrator owns workflow state. It accepts workflows, schedules the
 * steps whose dependencies are satisfied, and reacts to the lifecycle events
 * published by the pod manager (running, heartbeat, completed, failed).
 *
 * Invariant: this is the only component that writes step status. The pod
 * manager observes and reports; the orchestrator decides.
 */
export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<{ workflowId: string; status: string }> {
    assertValidDag(workflow)

    const state: WorkflowState = {
      workflowId: workflow.workflowId,
      status: "pending",
      steps: workflow.steps,
      stepState: {},
    }

    for (const step of workflow.steps) {
      state.stepState[step.id] = { stepId: step.id, status: "PENDING", podId: null }
    }

    // Persist before enqueueing: a result can arrive before submit returns,
    // and the result handler must be able to find the workflow.
    setWorkflow(state.workflowId, state)

    const ready = getReadySteps(workflow.steps, this.statusSnapshot(state))
    if (ready.length > 0) state.status = "running"
    await this.enqueue(state, ready)

    return { workflowId: workflow.workflowId, status: "accepted" }
  }

  async handleStepResult(result: StepResult): Promise<void> {
    const state = getWorkflow(result.workflowId)
    if (!state) {
      console.warn(`Ignoring result for unknown workflow ${result.workflowId}`)
      return
    }

    const stepState = state.stepState[result.stepId]
    if (!stepState) {
      console.warn(`Ignoring result for unknown step ${result.stepId} in ${result.workflowId}`)
      return
    }

    // Once a step is terminal its outcome is fixed. Late events (for example a
    // heartbeat that races a watchdog timeout) are dropped.
    if (isTerminal(stepState.status)) return

    switch (result.status) {
      case "RUNNING":
        stepState.status = "RUNNING"
        stepState.podId = result.podId
        stepState.startedAt = result.at
        stepState.lastHeartbeatAt = result.at
        if (state.status === "pending") state.status = "running"
        break

      case "HEARTBEAT":
        stepState.lastHeartbeatAt = result.at
        break

      case "COMPLETED":
        stepState.status = "COMPLETED"
        stepState.podId = result.podId
        stepState.stdout = result.stdout
        stepState.exitCode = result.exitCode
        await this.scheduleUnblockedSteps(state)
        break

      case "FAILED":
        stepState.status = "FAILED"
        stepState.podId = result.podId
        stepState.error = result.error
        stepState.exitCode = result.exitCode
        skipDependents(state, result.stepId)
        break
    }

    this.finalizeIfDone(state)
    setWorkflow(state.workflowId, state)
  }

  /**
   * Long-running steps emit a heartbeat on a fixed interval. If a step stops
   * reporting — typically because its pod crashed or was evicted — no further
   * events will ever arrive for it. This loop fails such steps so their
   * workflow can terminate instead of hanging forever.
   */
  private startLivenessMonitor(): void {
    setInterval(() => {
      const now = Date.now()

      for (const state of getAllWorkflows()) {
        if (state.status !== "running") continue
        let changed = false

        for (const stepState of Object.values(state.stepState)) {
          if (stepState.status !== "RUNNING") continue

          const lastSeen = stepState.lastHeartbeatAt ?? stepState.startedAt ?? now
          if (now - lastSeen <= LIVENESS_TIMEOUT_MS) continue

          const silentForMs = now - lastSeen
          console.warn(
            `Step ${state.workflowId}/${stepState.stepId} missed heartbeats for ${silentForMs}ms; marking failed`
          )
          stepState.status = "FAILED"
          stepState.error = `Pod stopped reporting after ${silentForMs}ms`
          skipDependents(state, stepState.stepId)
          changed = true
        }

        if (changed) {
          this.finalizeIfDone(state)
          setWorkflow(state.workflowId, state)
        }
      }
    }, LIVENESS_SCAN_INTERVAL_MS)
  }

  private async scheduleUnblockedSteps(state: WorkflowState): Promise<void> {
    const ready = getReadySteps(state.steps, this.statusSnapshot(state))
    await this.enqueue(state, ready)
  }

  private async enqueue(state: WorkflowState, steps: { id: string; command: string }[]): Promise<void> {
    for (const step of steps) {
      const queued: QueuedStep = {
        stepId: step.id,
        workflowId: state.workflowId,
        command: step.command,
        enqueuedAt: Date.now(),
      }
      state.stepState[step.id].status = "QUEUED"
      await stepQueue.enqueue(queued)
    }
    setWorkflow(state.workflowId, state)
  }

  private finalizeIfDone(state: WorkflowState): void {
    const states = Object.values(state.stepState)
    if (!states.every((step) => isTerminal(step.status))) return

    const failed = states.some((step) => step.status === "FAILED" || step.status === "SKIPPED")
    state.status = failed ? "failed" : "completed"
    console.log(`Workflow ${state.workflowId} finished: ${state.status}`)
  }

  private statusSnapshot(state: WorkflowState): Record<string, StepStatus> {
    return Object.fromEntries(
      Object.entries(state.stepState).map(([id, step]) => [id, step.status])
    )
  }

  start(): void {
    void this.consumeResults()
    void this.drainStepQueue()
    this.startLivenessMonitor()
  }

  private async consumeResults(): Promise<void> {
    await resultQueue.consume((result) => this.handleStepResult(result))
  }

  private async drainStepQueue(): Promise<void> {
    while (true) {
      const step = await stepQueue.dequeue()
      if (step) void this.dispatchStep(step)
    }
  }

  private async dispatchStep(step: QueuedStep): Promise<void> {
    try {
      await podManager.dispatch(step)
    } catch (err) {
      if (err instanceof Error && err.message === "NO_POD_AVAILABLE") {
        // The pool is fully leased; requeue and let a freed pod pick it up.
        await new Promise((resolve) => setTimeout(resolve, 100))
        await stepQueue.enqueue(step)
        return
      }
      console.error(`Failed to dispatch step ${step.stepId}:`, err)
    }
  }
}

export const orchestrator = new Orchestrator()
