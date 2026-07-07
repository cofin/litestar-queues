// docs: htmx-extension-start
import { registerHtmxExtension } from "litestar-vite-plugin/helpers"
import "htmx.org"
import "htmx-ext-sse"
import "./styles.css"

registerHtmxExtension()
// docs: htmx-extension-end

type QueueEvent = {
  type: string
  message?: string | null
  taskId?: string | null
  scope?: string | null
  scopeKey?: string | null
  progressCurrent?: number | null
  progressTotal?: number | null
  progressPercent?: number | null
  payload?: Record<string, unknown>
}

type StreamConnection = {
  close: () => void
}

const MISSION_SCOPE = "demo:mission-control"
const root = document.querySelector<HTMLElement>("[data-demo-root]")
let taskConnection: StreamConnection | null = null
let missionConnection: StreamConnection | null = null

function parseEventPayload(raw: string): QueueEvent | null {
  try {
    return JSON.parse(raw) as QueueEvent
  } catch {
    return null
  }
}

function setConnectionState(state: string): void {
  const target = document.querySelector<HTMLElement>("#connection-state")
  if (target) target.textContent = state
}

function eventLine(event: QueueEvent): string {
  if (event.message) return event.message
  if (typeof event.payload?.line === "string") return event.payload.line
  if (event.type === "task.completed") return "Mission complete."
  return event.type
}

function appendCrawlLine(event: QueueEvent): void {
  const target = document.querySelector<HTMLElement>("#crawl-lines")
  if (!target) return
  const line = document.createElement("p")
  line.textContent = eventLine(event)
  line.dataset.eventType = event.type
  target.append(line)
  while (target.children.length > 18) target.firstElementChild?.remove()
}

function resetCrawl(jobId?: string): void {
  const target = document.querySelector<HTMLElement>("#crawl-lines")
  if (!target) return
  const line = document.createElement("p")
  line.textContent = jobId ? `Mission ${jobId} queued.` : "Mission queued."
  target.replaceChildren(line)
  document.querySelector("#job-status")?.classList.remove("completed")
  setConnectionState("connecting")
}

function appendMissionLine(event: QueueEvent): void {
  const feed = document.querySelector<HTMLOListElement>("#mission-feed")
  if (!feed) return
  const item = document.createElement("li")
  item.textContent = eventLine(event)
  item.dataset.eventType = event.type
  feed.prepend(item)
  while (feed.children.length > 12) feed.lastElementChild?.remove()
}

function renderQueueEvent(event: QueueEvent, target: "task" | "mission"): void {
  if (event.type === "ping") return
  if (target === "mission" || event.scopeKey === MISSION_SCOPE) appendMissionLine(event)
  if (target === "task") appendCrawlLine(event)
  if (event.type === "task.completed") {
    document.querySelector("#job-status")?.classList.add("completed")
    setConnectionState("complete")
  }
}

// docs: sse-client-start
function connectSse(url: string, onEvent: (event: QueueEvent) => void): StreamConnection {
  const source = new EventSource(url)
  source.addEventListener("open", () => setConnectionState("sse"))
  const eventTypes = ["task.started", "task.progress", "task.log", "task.event", "task.completed", "mission.control"]
  for (const type of eventTypes) {
    source.addEventListener(type, (message) => {
      const event = parseEventPayload((message as MessageEvent<string>).data)
      if (event) onEvent(event)
    })
  }
  return { close: () => source.close() }
}
// docs: sse-client-end

function disconnectStreams(): void {
  taskConnection?.close()
  missionConnection?.close()
  taskConnection = null
  missionConnection = null
}

// docs: stream-adapter-start
function connectStreams(): void {
  const status = document.querySelector<HTMLElement>("#job-status")
  disconnectStreams()

  const missionSseUrl = status?.dataset.missionSseUrl ?? `/queues/events/sse/custom/${MISSION_SCOPE}`
  missionConnection = connectSse(missionSseUrl, (event) => renderQueueEvent(event, "mission"))

  const taskSseUrl = status?.dataset.taskSseUrl
  if (!taskSseUrl) return
  taskConnection = connectSse(taskSseUrl, (event) => renderQueueEvent(event, "task"))
}
// docs: stream-adapter-end

document.body.addEventListener("htmx:afterSwap", (event) => {
  if ((event.target as HTMLElement | null)?.id === "job-status") connectStreams()
})

document.body.addEventListener("queue-demo:started", (event) => {
  const detail = (event as CustomEvent<{ jobId?: string }>).detail
  resetCrawl(detail?.jobId)
  connectStreams()
})

if (root) connectStreams()
