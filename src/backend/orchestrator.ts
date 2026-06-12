import type { QueuedStep, StepResult, StepStatus, Workflow, WorkflowState } from "../types/workflow"
import { getWorkflow, setWorkflow } from "./workflow-store"
import { getReadySteps } from "./dag"
import { stepQueue } from "../queue/step-queue"
import { resultQueue } from "../queue/result-queue"
import { podManager } from "../pod-manager/pod-manager"

/**
 * The orchestrator owns workflow state. It accepts workflows, schedules the
 * steps whose dependencies are satisfied, and reacts to the lifecycle events
 * published by the pod manager.
 *
 * Invariant: this is the only component that writes step status. The pod
 * manager observes and reports; the orchestrator decides.
 */
export class Orchestrator {
  async submitWorkflow(workflow: Workflow): Promise<{ workflowId: string; status: string }> {
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
    if (!state) return

    const stepState = state.stepState[result.stepId]
    if (!stepState) return

    stepState.status = result.status
    stepState.podId = result.podId
    if (result.stdout !== undefined) stepState.stdout = result.stdout
    if (result.exitCode !== undefined) stepState.exitCode = result.exitCode
    if (result.error !== undefined) stepState.error = result.error

    if (result.status === "RUNNING" && state.status === "pending") {
      state.status = "running"
    }

    if (result.status === "COMPLETED") {
      const ready = getReadySteps(state.steps, this.statusSnapshot(state))
      await this.enqueue(state, ready)
    }

    this.finalizeIfDone(state)
    setWorkflow(state.workflowId, state)
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
    const done = states.every((step) => step.status === "COMPLETED" || step.status === "FAILED")
    if (!done) return

    state.status = states.some((step) => step.status === "FAILED") ? "failed" : "completed"
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
  }

  private async consumeResults(): Promise<void> {
    await resultQueue.consume((result) => this.handleStepResult(result))
  }

  // Dispatch is fire-and-forget, so independent steps run concurrently across
  // the pool instead of blocking each other.
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
