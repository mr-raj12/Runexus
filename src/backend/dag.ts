import type { StepStatus, Workflow, WorkflowStep, WorkflowState } from "../types/workflow"

const TERMINAL_STATUSES: ReadonlySet<StepStatus> = new Set(["COMPLETED", "FAILED", "SKIPPED"])

export function isTerminal(status: StepStatus): boolean {
  return TERMINAL_STATUSES.has(status)
}

/**
 * Returns the steps whose dependencies have all completed and that have not
 * yet been scheduled. A step with no dependencies is ready as soon as it is
 * pending.
 */
export function getReadySteps(
  steps: WorkflowStep[],
  stepStatus: Record<string, StepStatus>
): WorkflowStep[] {
  return steps.filter((step) => {
    if (stepStatus[step.id] !== "PENDING") return false
    if (!step.dependsOn || step.dependsOn.length === 0) return true
    return step.dependsOn.every((dependencyId) => stepStatus[dependencyId] === "COMPLETED")
  })
}

/**
 * Marks every step that transitively depends on a failed (or skipped) step as
 * SKIPPED. This guarantees a workflow reaches a terminal state instead of
 * stalling on dependents that can never become ready.
 */
export function skipDependents(state: WorkflowState, rootStepId: string): void {
  const queue: string[] = [rootStepId]

  while (queue.length > 0) {
    const blockedId = queue.shift() as string
    const dependents = state.steps.filter((step) => step.dependsOn?.includes(blockedId))

    for (const dependent of dependents) {
      const dependentState = state.stepState[dependent.id]
      if (!dependentState || isTerminal(dependentState.status)) continue

      dependentState.status = "SKIPPED"
      dependentState.error = `Skipped because dependency "${blockedId}" did not complete`
      queue.push(dependent.id)
    }
  }
}

/**
 * Validates structural integrity of a submitted workflow: a non-empty set of
 * uniquely identified steps, dependencies that reference real steps, and a
 * graph free of cycles. Throws on the first violation found.
 */
export function assertValidDag(workflow: Workflow): void {
  if (!workflow.workflowId || typeof workflow.workflowId !== "string") {
    throw new Error("workflowId is required")
  }

  if (!Array.isArray(workflow.steps) || workflow.steps.length === 0) {
    throw new Error("workflow must contain at least one step")
  }

  const stepIds = new Set<string>()
  for (const step of workflow.steps) {
    if (!step.id || !step.command) {
      throw new Error("each step requires an id and a command")
    }
    if (stepIds.has(step.id)) {
      throw new Error(`duplicate step id "${step.id}"`)
    }
    stepIds.add(step.id)
  }

  for (const step of workflow.steps) {
    for (const dependencyId of step.dependsOn ?? []) {
      if (!stepIds.has(dependencyId)) {
        throw new Error(`step "${step.id}" depends on unknown step "${dependencyId}"`)
      }
    }
  }

  assertAcyclic(workflow.steps)
}

function assertAcyclic(steps: WorkflowStep[]): void {
  const byId = new Map(steps.map((step) => [step.id, step]))
  const visited = new Set<string>()
  const onStack = new Set<string>()

  const visit = (stepId: string): void => {
    if (onStack.has(stepId)) {
      throw new Error(`workflow contains a cycle involving step "${stepId}"`)
    }
    if (visited.has(stepId)) return

    onStack.add(stepId)
    for (const dependencyId of byId.get(stepId)?.dependsOn ?? []) {
      visit(dependencyId)
    }
    onStack.delete(stepId)
    visited.add(stepId)
  }

  for (const step of steps) visit(step.id)
}
