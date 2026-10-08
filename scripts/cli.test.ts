import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import path from 'node:path'
import test from 'node:test'

const cwd = path.resolve(import.meta.dirname, '..')
const env = { PATH: process.env['PATH'] }

for (const args of [
  ['--help'],
  ['--version'],
  ['ingest', '--help'],
  ['build-cache', '--help'],
]) {
  test(`packaged CLI ${args.join(' ')} works without service credentials`, () => {
    const output = execFileSync(process.execPath, ['dist/main.js', ...args], {
      cwd,
      env,
      encoding: 'utf8',
      timeout: 10000,
    })
    assert.match(output, args[0] === '--version' ? /^1\.0\.0\s*$/ : /Usage:/)
    assert.doesNotMatch(output, /dotenv|Firebase|PrismaClient/)
  })
}

for (const client of ['api-users', 'api-media', 'users']) {
  test(`packaged ${client} Prisma client loads without connecting`, () => {
    execFileSync(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `const m = await import('./dist/lib/prisma/${client}/client.js'); if (typeof m.PrismaClient !== 'function') throw Error('missing PrismaClient')`,
      ],
      { cwd, env, timeout: 10000 }
    )
  })
}
