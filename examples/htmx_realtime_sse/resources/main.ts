// docs: htmx-extension-start
import htmx from "htmx.org"
import { registerHtmxExtension } from "litestar-vite-plugin/helpers"
import "./styles.css"

// htmx 2's ESM build never attaches itself to window, and the extensions read
// the global. Publish it first, register the litestar JSON extension, then load
// the SSE extension AFTER the global exists (a dynamic import runs in order,
// unlike a hoisted static import). Vite loads this module async, so it can run
// after DOMContentLoaded has already fired while readyState is "interactive" —
// htmx's own boot listener misses that window, so process the DOM explicitly.
;(window as unknown as { htmx: typeof htmx }).htmx = htmx
registerHtmxExtension()
void import("htmx-ext-sse").then(() => htmx.process(document.body))
// docs: htmx-extension-end

const MAX_LINES = 40
// After the final event, wait for the last lines to drift out of view, then
// return to the idle hint unless another mission starts first.
const IDLE_DELAY_MS = 45_000
const IDLE_TEXT = document.querySelector<HTMLElement>("#crawl-lines p")?.textContent ?? "Press restart to queue a background job."
let idleTimer: ReturnType<typeof setTimeout> | null = null

type QueueEvent = {
  type: string
  message?: string | null
  payload?: Record<string, unknown>
}

function parseQueueEvent(raw: string): QueueEvent | null {
  try {
    return JSON.parse(raw) as QueueEvent
  } catch {
    return null
  }
}

function eventLine(event: QueueEvent): string | null {
  if (event.message) return event.message
  if (typeof event.payload?.line === "string") return event.payload.line
  return null
}

function setReadout(text: string, completed = false): void {
  const readout = document.querySelector<HTMLElement>("#job-readout")
  if (!readout) return
  readout.textContent = text
  readout.classList.toggle("completed", completed)
}

function appendCrawlLine(event: QueueEvent): void {
  const target = document.querySelector<HTMLElement>("#crawl-lines")
  if (!target) return
  const text = eventLine(event)
  if (!text || target.lastElementChild?.textContent === text) return
  const line = document.createElement("p")
  line.textContent = text
  line.dataset.eventType = event.type
  target.append(line)
  while (target.children.length > MAX_LINES) target.firstElementChild?.remove()
}

// The one adapter the extensions cannot replace: queue frames are JSON, so they
// cannot be swapped as HTML. Parse each frame, ignore ping heartbeats, drop
// back-to-back duplicates (progress and custom events share a message), and
// append a crawl line. The terminal event only flips the readout — the task's
// own closing log line is the on-screen finale.
function handleQueueEvent(raw: string): void {
  const event = parseQueueEvent(raw)
  if (!event || event.type === "ping") return
  if (event.type === "task.completed") {
    setReadout("Mission complete", true)
    if (idleTimer) clearTimeout(idleTimer)
    idleTimer = setTimeout(goIdle, IDLE_DELAY_MS)
    return
  }
  appendCrawlLine(event)
}

function goIdle(): void {
  const plane = document.querySelector<HTMLElement>("#crawl-lines")
  if (!plane) return
  const hint = document.createElement("p")
  hint.textContent = IDLE_TEXT
  plane.replaceChildren(hint)
  plane.classList.add("idle")
  setReadout("Awaiting launch")
}

function resetCrawl(jobId?: string): void {
  if (idleTimer) clearTimeout(idleTimer)
  idleTimer = null
  const plane = document.querySelector<HTMLElement>("#crawl-lines")
  if (plane) {
    plane.classList.remove("idle")
    plane.replaceChildren()
    // The crawl keyframe is one-shot and time-based; restart it so every
    // mission's lines enter from the bottom instead of mid-flight.
    plane.style.animation = "none"
    void plane.offsetHeight
    plane.style.animation = ""
  }
  setReadout(jobId ? `Mission ${jobId} underway` : "Mission underway")
}

// docs: sse-client-start
// htmx-ext-sse fires htmx:sseBeforeMessage (cancelable) for each named frame in
// sse-swap before it would swap. detail is the raw MessageEvent; preventDefault
// keeps htmx from injecting JSON into the DOM.
document.body.addEventListener("htmx:sseBeforeMessage", (event) => {
  event.preventDefault()
  const message = (event as CustomEvent<MessageEvent<string>>).detail
  handleQueueEvent(message.data)
})
// docs: sse-client-end

// docs: stream-adapter-start
// HTMXTemplate(trigger_event="queue-demo:started") fires after the restart swap
// replaces #stream-mount. Swapping the element reconnects the stream (the old
// EventSource closes with the removed element); reset the crawl to match.
document.body.addEventListener("queue-demo:started", (event) => {
  const detail = (event as CustomEvent<{ jobId?: string }>).detail
  resetCrawl(detail?.jobId)
})
// docs: stream-adapter-end
