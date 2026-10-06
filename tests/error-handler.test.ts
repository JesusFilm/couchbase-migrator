import assert from 'node:assert/strict'
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import test from 'node:test'
import {
  clearErrorsDirectory,
  writeErrorToFile,
} from '../src/lib/error-handler.js'
import { Logger } from '../src/lib/logger.js'

const logger = new Logger()

test('failed records retain the error and payload for a later retry', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'migrator-errors-'))
  try {
    const payload = { id: 'user-42', email: 'fixture@example.invalid' }
    await writeErrorToFile(
      dir,
      'users',
      '/cache/users/42.json',
      new Error('invalid email'),
      logger,
      payload
    )
    const saved = JSON.parse(
      await readFile(path.join(dir, 'errors/users/42.json'), 'utf8')
    )
    assert.deepEqual(saved, { error: 'invalid email', data: payload })
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('clearing one pipeline preserves errors for the other pipeline', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'migrator-errors-'))
  try {
    await writeErrorToFile(dir, 'users', '42.json', 'invalid user', logger)
    await writeErrorToFile(
      dir,
      'playlists',
      '17.json',
      'invalid playlist',
      logger
    )
    await clearErrorsDirectory(dir, 'users', logger)
    assert.deepEqual(await readdir(path.join(dir, 'errors/users')), [])
    assert.deepEqual(
      JSON.parse(
        await readFile(path.join(dir, 'errors/playlists/17.json'), 'utf8')
      ),
      { error: 'invalid playlist' }
    )
    await clearErrorsDirectory(dir, 'users', logger)
    await writeFile(path.join(dir, 'errors/users/new.json'), '{}')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('an unwritable error destination is reported without hiding the migration failure', async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'migrator-errors-'))
  try {
    await writeFile(path.join(dir, 'errors'), 'blocks directory creation')
    const errors: unknown[][] = []
    const recordingLogger = new Logger()
    recordingLogger.error = (...args: unknown[]) => {
      errors.push(args)
    }
    await writeErrorToFile(
      dir,
      'users',
      '42.json',
      new Error('migration failure'),
      recordingLogger
    )
    assert.equal(errors.length, 1)
    assert.match(
      String(errors[0]?.[0]),
      /Failed to write error file for 42.json/
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})
