export type StepStatus = "PENDING" | "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "SKIPPED"

export type WorkflowStatus = "pending" | "running" | "completed" | "failed"

export type WorkflowStep = {
  id: string
  command: string
  dependsOn?: string[]
}

export type Workflow = {
  workflowId: string
  steps: WorkflowStep[]
}

export type StepState = {
  stepId: string
  status: StepStatus
  podId: string | null
  stdout?: string
  exitCode?: number
  error?: string
  startedAt?: number
  lastHeartbeatAt?: number
}

export type WorkflowState = {
  workflowId: string
  status: WorkflowStatus
  steps: WorkflowStep[]
  stepState: Record<string, StepState>
}

export type QueuedStep = {
  stepId: string
  workflowId: string
  command: string
  enqueuedAt: number
}

export type StepResultStatus = "RUNNING" | "HEARTBEAT" | "COMPLETED" | "FAILED"

export type StepResult = {
  stepId: string
  workflowId: string
  podId: string
  status: StepResultStatus
  at: number
  stdout?: string
  exitCode?: number
  error?: string
}

export type Pod = {
  podId: string
  podName: string
  namespace: string
}

export type PoolStatus = {
  total: number
  available: number
  leased: string[]
}
