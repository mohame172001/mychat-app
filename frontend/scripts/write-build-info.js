const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

function gitSha() {
  if (process.env.REACT_APP_GIT_SHA) return process.env.REACT_APP_GIT_SHA.slice(0, 12);
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
