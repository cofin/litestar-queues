import litestar from "litestar-vite-plugin"
import { defineConfig, version } from "vite"

const bundlerKey = Number(version.split(".")[0]) >= 8 ? "rolldownOptions" : "rollupOptions"

export default defineConfig({
  clearScreen: false,
  // One bundle serves all ten example apps. Each entry is a transport: the SSE
  // apps load main-sse.ts, the WebSocket apps load main-ws.ts, and base.html
  // picks between them from the `vite_entry` its app passes in.
  plugins: [litestar({ input: ["resources/main-sse.ts", "resources/main-ws.ts"] })],
  build: {
    [bundlerKey]: {
      onwarn(warning, warn) {
        if (warning.code === "EVAL" && warning.id?.includes("htmx")) return
        warn(warning)
      },
    },
  },
})
