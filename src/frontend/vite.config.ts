import { defineConfig, loadEnv, type UserConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';
import { createRequire } from 'node:module';

const { preparePublic } = createRequire(import.meta.url)('../scripts/build-tasks.cjs');

/**
 * The local backend's port: VITE_KASAL_PORT, else KASAL_PORT (what run.sh
 * reads), else 8000. Used for the /api proxy, and handed to the browser as
 * import.meta.env.VITE_KASAL_PORT (see src/shared/api/backendOrigin.ts).
 */
function resolveBackendPort(env: Record<string, string>): string {
  const raw = (env.VITE_KASAL_PORT || env.KASAL_PORT || '').trim();
  const port = Number(raw);
  return /^\d+$/.test(raw) && port > 0 && port <= 65535 ? raw : '8000';
}

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const analyze = process.env.ANALYZE === 'true';
  // '' prefix: read KASAL_PORT as well as VITE_* from .env files and the shell.
  const backendPort = resolveBackendPort(loadEnv(mode, __dirname, ''));

  const config: UserConfig = {
    // Development, direct Vite builds and npm builds all use the same docs
    // source without writing generated copies into the tracked public tree.
    publicDir: preparePublic(path.resolve(__dirname, '..')),
    plugins: [
      // disableOxcRecommendation: silence the "switch to @vitejs/plugin-react-oxc"
      // notice. That plugin is now DEPRECATED (folded back into plugin-react), and
      // the plugin-react version that includes it (6.x) requires Vite 8 — which
      // this pinned rolldown-vite 6.3.21 toolchain can't use. So the recommendation
      // is unactionable here; this is the plugin's own supported way to mute it.
      react({ disableOxcRecommendation: true }),
      // NOTE: precompression (gzip/brotli) plugins removed — the app server
      // (entrypoint.py) does not serve .gz/.br, so those artifacts were unused
      // dead weight and brotli compression alone exhausted the Databricks Apps
      // build container's ~2GB Node heap. Compression is handled at the edge.
      // Bundle analyzer
      analyze && visualizer({
        filename: 'bundle-report.html',
        open: true,
        gzipSize: true,
        brotliSize: true,
      }),
    ].filter(Boolean),

    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },

    server: {
      port: 3000,
      open: true,
      proxy: {
        '/api': {
          target: `http://localhost:${backendPort}`,
          changeOrigin: true,
        },
      },
    },

    preview: {
      port: 3000,
    },

    build: {
      outDir: 'dist',
      sourcemap: false,
      // Minifier: terser. rolldown-vite's default oxc minifier MIS-MANGLES this
      // bundle — it emits a duplicate `const n` in one chunk, producing a runtime
      // "Identifier 'n' has already been declared" SyntaxError that blanks the
      // app. esbuild is no longer usable (rolldown dropped transformWithEsbuild).
      // terser mangles correctly; the 4GB NODE_OPTIONS heap (build script) covers
      // its higher memory use.
      minify: 'terser',
      // Keep debug logging out of production. console.log/info/debug/trace and
      // `debugger` are dropped; console.warn and console.error are kept so
      // real problems still reach the browser console. (Stripping was lost when
      // the build briefly moved to esbuild and then oxc, neither of which could
      // express it here; terser can, and it is the minifier again.)
      terserOptions: {
        compress: {
          drop_console: ['log', 'info', 'debug', 'trace'],
          drop_debugger: true,
        },
      },
      // Manual vendor/mui/redux chunk grouping removed: rolldown-vite types
      // reject the object form of manualChunks, and rolldown's default
      // chunking already splits vendors sensibly. Re-add via
      // output.advancedChunks if finer control is ever needed.
      chunkSizeWarningLimit: 512,
      // Skip the post-build "computing gzip size..." pass: it gzips every
      // chunk in memory just to print a size summary and OOMs the Apps build
      // container's V8 heap on this bundle. Purely cosmetic; safe to disable.
      reportCompressedSize: false,
    },

    optimizeDeps: {
      include: ['react', 'react-dom', 'react-router-dom'],
    },

    define: {
      // Handle process.env for libraries that might use it
      'process.env': {},
      // So `KASAL_PORT=8001 npm start` reaches a backend started the same way.
      'import.meta.env.VITE_KASAL_PORT': JSON.stringify(backendPort),
    },
  };
  return config;
});
