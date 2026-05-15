const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Phase 2.18P: pick up Railway-provided git SHA (Railway exposes
// RAILWAY_GIT_COMMIT_SHA in the build env), fall back to manual
// REACT_APP_GIT_SHA, and only then to a local `git rev-parse`. The
// previous "local" string was leaking into the production footer
// because the Railway builder runs inside a context where the
// `git` command cannot find the .git directory.
function gitSha() {
  const candidates = [
    process.env.REACT_APP_GIT_SHA,
    process.env.RAILWAY_GIT_COMMIT_SHA,
    process.env.RAILWAY_DEPLOYMENT_ID,
    process.env.SOURCE_COMMIT,
    process.env.VERCEL_GIT_COMMIT_SHA,
  ];
  for (const candidate of candidates) {
    if (candidate && typeof candidate === 'string' && candidate.trim()) {
      return candidate.trim().slice(0, 12);
    }
  }
  try {
    return execSync('git rev-parse --short=12 HEAD', {
      cwd: path.resolve(__dirname, '..', '..'),
      stdio: ['ignore', 'pipe', 'ignore'],
    }).toString().trim();
  } catch (_err) {
    return 'local';
  }
}

const outPath = path.resolve(__dirname, '..', 'src', 'buildInfo.generated.js');
fs.writeFileSync(outPath, `export const BUILD_SHA = '${gitSha()}';\n`);
