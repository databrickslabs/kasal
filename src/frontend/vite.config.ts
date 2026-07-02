import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const analyze = process.env.ANALYZE === 'true';

  return {
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
      // esbuild minifier (not terser): terser exhausts the Databricks Apps
      // build container's ~2GB Node heap on this bundle. esbuild uses a
      // fraction of the memory. Console/debugger stripping is preserved via
      // the top-level `esbuild.drop` option below.
      minify: 'esbuild',
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
    // terserOptions.compress; esbuild's `drop` is the equivalent).
    esbuild: {
      drop: ['console', 'debugger'],
    },

    optimizeDeps: {
      include: ['react', 'react-dom', 'react-router-dom'],
    },

    define: {
      // Handle process.env for libraries that might use it
      'process.env': {},
    },
  };
});
