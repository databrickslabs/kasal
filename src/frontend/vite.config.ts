import { defineConfig, type UserConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const analyze = process.env.ANALYZE === 'true';

  const config: UserConfig = {
    plugins: [
      react(),
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
          target: 'http://localhost:8000',
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

    // Strip console.*/debugger from production bundles (previously handled by
    // terserOptions.compress; esbuild's `drop` is the equivalent). The literal
    // cast is required because rolldown-vite types `drop` as
    // ('console' | 'debugger')[], and a bare array infers as string[].
    // NOTE: console/debugger stripping via `esbuild.drop` was removed —
    // rolldown-vite dropped the esbuild transform (transformWithEsbuild) and the
    // block broke the build. Stripping is cosmetic (dev-noise only); the default
    // oxc minifier still fully minifies the bundle. Re-add via oxc minify options
    // if console removal is required.

    optimizeDeps: {
      include: ['react', 'react-dom', 'react-router-dom'],
    },

    define: {
      // Handle process.env for libraries that might use it
      'process.env': {},
    },
  };
  return config;
});
