// craco.config.js
const path = require("path");
const webpack = require("webpack");
require("dotenv").config();

// Check if we're in development/preview mode (not production build)
// Craco sets NODE_ENV=development for start, NODE_ENV=production for build
const isDevServer = process.env.NODE_ENV !== "production";

// Phase 2.18B: optional analytics/observability SDKs are loaded via
// dynamic import() in src/lib/{analytics,sentryClient}.js with a
// .catch(() => null) fallback. They are truly optional at runtime
// (no DSN/key in env → no init). However, webpack still tries to
// resolve their import strings at build time, and CI=true treats any
// "Module not found" warning as a hard error. To keep production
// builds robust whether or not these SDKs are installed, we register
// an IgnorePlugin for *each* dep that is genuinely missing from
// node_modules. If the dep IS installed, the IgnorePlugin is NOT
// added, so the SDK is bundled normally and lazy-loaded at runtime.
const OPTIONAL_RUNTIME_DEPS = ["@sentry/react", "posthog-js"];
const _missingOptionalDeps = OPTIONAL_RUNTIME_DEPS.filter((dep) => {
  try {
    require.resolve(dep, { paths: [path.resolve(__dirname)] });
    return false;
  } catch (_) {
    return true;
  }
});

// Environment variable overrides
const config = {
  enableHealthCheck: process.env.ENABLE_HEALTH_CHECK === "true",
};

// Conditionally load health check modules only if enabled
let WebpackHealthPlugin;
let setupHealthEndpoints;
let healthPluginInstance;

if (config.enableHealthCheck) {
  WebpackHealthPlugin = require("./plugins/health-check/webpack-health-plugin");
  setupHealthEndpoints = require("./plugins/health-check/health-endpoints");
  healthPluginInstance = new WebpackHealthPlugin();
}

let webpackConfig = {
  eslint: {
    configure: {
      extends: ["plugin:react-hooks/recommended"],
      rules: {
        "react-hooks/rules-of-hooks": "error",
        "react-hooks/exhaustive-deps": "warn",
      },
    },
  },
  webpack: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
    configure: (webpackConfig) => {

      // Add ignored patterns to reduce watched directories
        webpackConfig.watchOptions = {
          ...webpackConfig.watchOptions,
          ignored: [
            '**/node_modules/**',
            '**/.git/**',
            '**/build/**',
            '**/dist/**',
            '**/coverage/**',
            '**/public/**',
        ],
      };

      // Add health check plugin to webpack if enabled
      if (config.enableHealthCheck && healthPluginInstance) {
        webpackConfig.plugins.push(healthPluginInstance);
      }

      // Phase 2.18B: Ignore missing optional SDKs so CI=true builds
      // do not fail on "Module not found" for deps that are loaded
      // through guarded dynamic import() with .catch(() => null).
      // Only ignores deps that are actually absent from node_modules;
      // installed deps are bundled normally as lazy chunks.
      if (_missingOptionalDeps.length > 0) {
        const escapedNames = _missingOptionalDeps.map((dep) =>
          dep.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
        );
        const ignorePattern = new RegExp(
          `^(${escapedNames.join("|")})$`
        );
        webpackConfig.plugins = webpackConfig.plugins || [];
        webpackConfig.plugins.push(
          new webpack.IgnorePlugin({ resourceRegExp: ignorePattern })
        );
        // One-shot informational log so ops can see the decision in CI logs.
        // No secrets, no env values — only the dep names.
        // eslint-disable-next-line no-console
        console.warn(
          `[craco] optional runtime deps not installed; webpack will ignore: ${_missingOptionalDeps.join(", ")}`
        );
      }
      return webpackConfig;
    },
  },
};

webpackConfig.devServer = (devServerConfig) => {
  // Add health check endpoints if enabled
  if (config.enableHealthCheck && setupHealthEndpoints && healthPluginInstance) {
    const originalSetupMiddlewares = devServerConfig.setupMiddlewares;

    devServerConfig.setupMiddlewares = (middlewares, devServer) => {
      // Call original setup if exists
      if (originalSetupMiddlewares) {
        middlewares = originalSetupMiddlewares(middlewares, devServer);
      }

      // Setup health endpoints
      setupHealthEndpoints(devServer, healthPluginInstance);

      return middlewares;
    };
  }

  return devServerConfig;
};

// Wrap with visual edits (automatically adds babel plugin, dev server, and overlay in dev mode)
if (isDevServer) {
  try {
    const { withVisualEdits } = require("@emergentbase/visual-edits/craco");
    webpackConfig = withVisualEdits(webpackConfig);
  } catch (err) {
    if (err.code === 'MODULE_NOT_FOUND' && err.message.includes('@emergentbase/visual-edits/craco')) {
      console.warn(
        "[visual-edits] @emergentbase/visual-edits not installed — visual editing disabled."
      );
    } else {
      throw err;
    }
  }
}

module.exports = webpackConfig;
