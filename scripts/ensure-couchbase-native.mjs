import { createRequire } from 'node:module'
import { execFileSync } from 'node:child_process'

// Frozen discovery installs intentionally skip lifecycle scripts. A packaged
// operational command must load the real native SDK before a build can pass.
const require = createRequire(import.meta.url)
try {
  require('couchbase')
} catch {
  execFileSync('pnpm', ['rebuild', 'couchbase'], { stdio: 'inherit' })
  require('couchbase')
}
