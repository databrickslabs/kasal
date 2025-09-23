const TerserPlugin = require('terser-webpack-plugin');
const CompressionPlugin = require('compression-webpack-plugin');
const { BundleAnalyzerPlugin } = require('webpack-bundle-analyzer');
const webpack = require('webpack');
const path = require('path');

module.exports = {
  webpack: {
    configure: (webpackConfig, { env, paths }) => {
      const isProduction = env === 'production';

      // Optimization configuration for production
      if (isProduction) {
        // Enhanced optimization settings
        webpackConfig.optimization = {
          ...webpackConfig.optimization,
          minimize: true,
          minimizer: [
            new TerserPlugin({
              terserOptions: {
                parse: {
                  ecma: 2020,
                },
                compress: {
                  ecma: 2015,
                  warnings: false,
                  comparisons: false,
                  inline: 2,
                  drop_console: true,
                  drop_debugger: true,
                  pure_funcs: ['console.log', 'console.info', 'console.debug', 'console.warn'],
                },
                mangle: {
                  safari10: true,
                },
                output: {
                  ecma: 2015,
                  comments: false,
                  ascii_only: true,
                },
              },
              parallel: true,
              extractComments: false,
            }),
          ],
          splitChunks: {
            chunks: 'all',
            cacheGroups: {
              vendor: {
                test: /[\\/]node_modules[\\/]/,
                name: 'vendor',
                priority: 10,
              },
              default: {
                minChunks: 2,
                priority: -20,
                reuseExistingChunk: true,
              },
            },
          },
        };

        // Add compression plugin for gzip and brotli
        webpackConfig.plugins.push(
          new CompressionPlugin({
            filename: '[path][base].gz',
            algorithm: 'gzip',
            test: /\.(js|css|html|svg|json)$/,
            threshold: 8192,
            minRatio: 0.8,
          }),
          new CompressionPlugin({
            filename: '[path][base].br',
            algorithm: 'brotliCompress',
            test: /\.(js|css|html|svg|json)$/,
            compressionOptions: {
              level: 11,
            },
            threshold: 10240,
            minRatio: 0.8,
          })
        );


        // Ignore moment.js locales to reduce bundle size
        webpackConfig.plugins.push(
          new webpack.IgnorePlugin({
            resourceRegExp: /^\.\/locale$/,
            contextRegExp: /moment$/,
          })
        );
      }

      // Add Bundle Analyzer only when ANALYZE environment variable is set
      if (process.env.ANALYZE === 'true') {
        webpackConfig.plugins.push(
          new BundleAnalyzerPlugin({
            analyzerMode: 'static',
            reportFilename: 'bundle-report.html',
            openAnalyzer: true,
            generateStatsFile: true,
            statsFilename: 'bundle-stats.json',
          })
        );
      }

      // Module resolution optimization
      webpackConfig.resolve = {
        ...webpackConfig.resolve,
        alias: {
          ...webpackConfig.resolve.alias,
          '@': path.resolve(__dirname, 'src'),
        },
        // Prioritize browser-friendly entry points for bundling
        mainFields: ['browser', 'module', 'main'],
        // Optimize module resolution
        extensions: ['.ts', '.tsx', '.js', '.jsx', '.json'],
        // Polyfills / fallbacks for Node core modules used by some deps
        fallback: {
          ...(webpackConfig.resolve?.fallback || {}),
          util: require.resolve('util/'),
          tty: false,
        },
      };

      // Performance hints
      webpackConfig.performance = {
        hints: isProduction ? 'warning' : false,
        maxEntrypointSize: 512000,
        maxAssetSize: 512000,
      };

      // Enable source maps only for development
      if (!isProduction) {
        webpackConfig.devtool = 'eval-source-map';
      } else {
        // Disable source maps in production (already handled by GENERATE_SOURCEMAP=false)
        webpackConfig.devtool = false;
      }

      return webpackConfig;
    },
  },
  // TypeScript configuration
  typescript: {
    enableTypeChecking: true,
  },
  // Babel configuration
  babel: {
    presets: [
      [
        '@babel/preset-env',
        {
          useBuiltIns: 'entry',
          corejs: 3,
          modules: false,
          targets: {
            browsers: ['>0.2%', 'not dead', 'not op_mini all'],
          },
        },
      ],
    ],
    plugins: [],
  },
};