import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// In dev, proxy the agent endpoint to the backend (AGENT_PORT, default 8001 for
// local dev). In production the built SPA is served directly by the agent server
// (StaticFiles), so `/invocations` is already same-origin and no proxy is needed.
const AGENT_PORT = process.env.AGENT_PORT ?? '8001'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      // shadcn/ui convention: "@/..." -> ./src
      '@': fileURLToPath(new URL('./src', import.meta.url)),
      // Single source of truth: the SAME catalog the backend composer reads.
      '@catalog': fileURLToPath(
        new URL('../agent_server/a2ui_catalog.json', import.meta.url),
      ),
    },
  },
  server: {
    port: 5173,
    // Allow importing the catalog from the sibling agent_server/ dir.
    fs: { allow: ['..'] },
    proxy: {
      '/invocations': {
        target: `http://localhost:${AGENT_PORT}`,
        changeOrigin: true,
        timeout: 600000,
        proxyTimeout: 600000,
      },
      '/me': { target: `http://localhost:${AGENT_PORT}`, changeOrigin: true },
      '/progress': { target: `http://localhost:${AGENT_PORT}`, changeOrigin: true },
      '/cancel': { target: `http://localhost:${AGENT_PORT}`, changeOrigin: true },
      '/a2ui': { target: `http://localhost:${AGENT_PORT}`, changeOrigin: true },
      '/conversations': {
        target: `http://localhost:${AGENT_PORT}`,
        changeOrigin: true,
      },
    },
  },
  preview: { port: 3000, host: true },
})
