import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { visualizer } from 'rollup-plugin-visualizer';
import viteCompression from 'vite-plugin-compression';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const isProduction = mode === 'production';
  const analyze = process.env.ANALYZE === 'true';

  return {
    plugins: [
      react(),
      // Gzip compression
      isProduction && viteCompression({
        algorithm: 'gzip',
        ext: '.gz',
        threshold: 8192,
      }),
      // Brotli compression
      isProduction && viteCompression({
        algorithm: 'brotliCompress',
        ext: '.br',
        threshold: 10240,
      }),
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
      rollupOptions: {
        output: {
          manualChunks: {
            vendor: ['react', 'react-dom', 'react-router-dom'],
            mui: ['@mui/material', '@mui/icons-material'],
            redux: ['@reduxjs/toolkit', 'react-redux', 'redux'],
          },
        },
      },
      chunkSizeWarningLimit: 512,
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
