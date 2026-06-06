import { CoreV1Api, KubeConfig } from "@kubernetes/client-node"
import type { Pod, PoolStatus } from "../types/workflow"

const DEFAULT_NAMESPACE = "workflow-runner"
const DEFAULT_LABEL_SELECTOR = "app=runner"
const RUNNER_CONTAINER_NAME = "runner"
const ACQUIRING = "__acquiring__"

interface MutableCluster {
  skipTLSVerify: boolean
}

interface MutableUser {
  token?: string
}

function podIdFromName(podName: string): string {
  return podName.startsWith("runner-") ? podName.slice("runner-".length) : podName
}

function serviceAccountToken(namespace: string): string | undefined {
  const result = Bun.spawnSync(["kubectl", "create", "token", "default", "-n", namespace])
  if (result.exitCode !== 0) return undefined
  const token = result.stdout.toString().trim()
  return token.length > 0 ? token : undefined
}

/**
 * A fixed pool of pre-warmed runner pods. Steps lease a pod for the duration
 * of their command and release it afterwards, so the cluster never has to
 * schedule a pod on the hot path.
 *
 * Reads (discovery, liveness) go through the Kubernetes API. Command execution
 * shells out to `kubectl exec` asynchronously, which keeps the event loop free
 * while a step runs and avoids the websocket exec channel that kind's local
 * TLS setup makes unreliable.
 */
export class PodPool {
  private readonly namespace = process.env.KUBE_NAMESPACE || DEFAULT_NAMESPACE
  private readonly labelSelector = process.env.KUBE_POD_LABEL || DEFAULT_LABEL_SELECTOR
  private readonly leases = new Map<string, string | null>()
  private readonly pods = new Map<string, Pod>()
  private readonly coreV1Api: CoreV1Api

  readonly ready: Promise<void>

  constructor() {
    const kubeConfig = new KubeConfig()
    kubeConfig.loadFromDefault()

    const cluster = kubeConfig.getCurrentCluster()
    if (cluster) {
      ;(cluster as unknown as MutableCluster).skipTLSVerify = true
    }

    const user = kubeConfig.getCurrentUser()
    const token = serviceAccountToken(this.namespace)
    if (user && token) {
      ;(user as unknown as MutableUser).token = token
    }

    this.coreV1Api = kubeConfig.makeApiClient(CoreV1Api)
    this.ready = this.discover()
  }

  async acquirePod(): Promise<Pod> {
    await this.ready

    for (const [podId, leasedBy] of this.leases) {
      if (leasedBy !== null) continue

      this.leases.set(podId, ACQUIRING)
      const pod = this.pods.get(podId)
      if (!pod) throw new Error(`Pod ${podId} is missing from the pool`)
      return pod
    }

    throw new Error("NO_POD_AVAILABLE")
  }

  async releasePod(podId: string): Promise<void> {
    await this.ready
    if (!this.leases.has(podId)) throw new Error(`Unknown pod id: ${podId}`)
    this.leases.set(podId, null)
  }

  async execInPod(podId: string, command: string): Promise<string> {
    await this.ready

    const podName = this.podName(podId)
    const proc = Bun.spawn(
      ["kubectl", "exec", "-n", this.namespace, podName, "-c", RUNNER_CONTAINER_NAME, "--", "sh", "-c", command],
      { stdout: "pipe", stderr: "pipe" }
    )

    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])

    if (exitCode !== 0) {
      throw new Error(stderr.trim() || `command exited with code ${exitCode}`)
    }
    return stdout
  }

  async isPodAlive(podId: string): Promise<boolean> {
    try {
      const pod = await this.coreV1Api.readNamespacedPod({ name: this.podName(podId), namespace: this.namespace })
      return pod.status?.phase === "Running"
    } catch {
      return false
    }
  }

  getPoolStatus(): PoolStatus {
    const leased = [...this.leases.entries()]
      .filter(([, leasedBy]) => leasedBy !== null)
      .map(([podId]) => podId)

    return {
      total: this.leases.size,
      available: this.leases.size - leased.length,
      leased,
    }
  }

  private podName(podId: string): string {
    return this.pods.get(podId)?.podName ?? `runner-${podId}`
  }

  private async discover(): Promise<void> {
    const podList = await this.coreV1Api.listNamespacedPod({
      namespace: this.namespace,
      labelSelector: this.labelSelector,
    })

    const pods = podList.items
      .map((pod) => pod.metadata?.name)
      .filter((name): name is string => Boolean(name))
      .sort()
      .map((name) => ({ podId: podIdFromName(name), podName: name, namespace: this.namespace }))

    this.pods.clear()
    this.leases.clear()
    for (const pod of pods) {
      this.pods.set(pod.podId, pod)
      this.leases.set(pod.podId, null)
    }

    console.log(`Discovered ${pods.length} runner pods in namespace ${this.namespace}`)
  }
}

export const podPool = new PodPool()
