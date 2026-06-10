import express from "express"
import type { Workflow } from "../types/workflow"
import { podPool } from "../k8s/pod-pool"
import { orchestrator } from "./orchestrator"
import { getWorkflow } from "./workflow-store"

const app = express()
app.use(express.json())

// Accept a workflow for execution.
app.post("/workflow", async (req, res) => {
  const result = await orchestrator.submitWorkflow(req.body as Workflow)
  res.status(202).json(result)
})

// Current state of a workflow and each of its steps.
app.get("/workflow/:id", (req, res) => {
  const workflow = getWorkflow(req.params.id)
  if (!workflow) {
    res.status(404).json({ error: "workflow not found" })
    return
  }

  res.json({
    workflowId: workflow.workflowId,
    status: workflow.status,
    steps: workflow.steps.map((step) => {
      const state = workflow.stepState[step.id]
      return {
        id: step.id,
        status: state.status,
        podId: state.podId,
        exitCode: state.exitCode,
        stdout: state.stdout,
        error: state.error,
      }
    }),
  })
})

// Pool occupancy, handy for debugging dispatch and back-pressure.
app.get("/pods", (_req, res) => {
  res.json(podPool.getPoolStatus())
})

export { app }
