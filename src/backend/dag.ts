import type { StepStatus, WorkflowStep } from "../types/workflow"

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
