import litestar from "litestar-vite-plugin"
import { defineConfig, version } from "vite"

const bundlerKey = Number(version.split(".")[0]) >= 8 ? "rolldownOptions" : "rollupOptions"

export default defineConfig({
  clearScreen: false,
  plugins: [litestar({ input: ["resources/main.ts"] })],
  build: {
    [bundlerKey]: {
      onwarn(warning, warn) {
        if (warning.code === "EVAL" && warning.id?.includes("htmx")) return
        warn(warning)
      },
    },
  },
})
