#!/usr/bin/env node

/**
 * Couchbase Migrator - Main Entry Point
 *
 * This is the main entry point for the Couchbase migration tool.
 */

import { Command } from 'commander'
import { buildCache } from './commands/buildCache.js'
import { ingest } from './commands/ingest.js'

// Create commander program
const program = new Command()

program
  .name('couchbase-migrator')
  .description('Document migration tool for Couchbase')
  .version('1.0.0')

// Build cache subcommand
program
  .command('build-cache')
  .description('Build document cache by migrating documents from Couchbase')
  .option(
    '--skip-attachments',
    'skip processing binary attachments (only process JSON documents)'
  )
  .action(async options => {
    try {
      await buildCache(options)
    } catch (error) {
      console.error('❌ Fatal error:', error)
      process.exit(1)
    }
  })

// Ingest subcommand
program
  .command('ingest')
  .description('Ingest documents from cache into Core')
  .option(
    '--source-dir <path>',
    'source directory for cached documents',
    './tmp'
  )
  .option(
    '--pipeline <type>',
    'specify which pipeline to run: users, playlists, or all',
    'all'
  )
  .option('--dry-run', 'perform a dry run without actually ingesting data')
  .action(async options => {
    try {
      await ingest(options)
    } catch (error) {
      console.error('❌ Fatal error:', error)
      process.exit(1)
    }
  })

// Parse command line arguments and execute program
if (import.meta.url === `file://${process.argv[1]}`) {
  program.parse()
}
