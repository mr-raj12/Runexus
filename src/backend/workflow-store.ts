import type { WorkflowState } from "../types/workflow"

const store = new Map<string, WorkflowState>()

export function getWorkflow(workflowId: string): WorkflowState | undefined {
  return store.get(workflowId)
}

export function setWorkflow(workflowId: string, state: WorkflowState): void {
  store.set(workflowId, state)
}

export function getAllWorkflows(): WorkflowState[] {
  return Array.from(store.values())
}
