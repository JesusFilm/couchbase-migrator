/**
 * Ingest Module
 *
 * Handles ingesting documents from the cache into Core
 */

import { ingestUsers } from './ingest/users.js'
import { ingestPlaylists } from './ingest/playlists.js'

export interface IngestOptions {
  sourceDir?: string
  dryRun?: boolean
  pipeline?: 'users' | 'playlists' | 'all'
}

/**
 * Ingest documents from cache into Core
 * @param options Options for ingestion
 */
export async function ingest(options: IngestOptions = {}): Promise<void> {
  const { sourceDir = './tmp', dryRun = false, pipeline = 'all' } = options

  console.log('🚀 Starting document ingestion...')
  console.log(`📁 Source directory: ${sourceDir}`)
  console.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)
  console.log(`🎯 Pipeline: ${pipeline}`)

  try {
    switch (pipeline) {
      case 'users':
        await ingestUsers({ sourceDir, dryRun })
        break

      case 'playlists':
        await ingestPlaylists({ sourceDir, dryRun })
        break

      case 'all':
        await ingestUsers({ sourceDir, dryRun })
        await ingestPlaylists({ sourceDir, dryRun })
        break

      default:
        throw new Error(`Invalid pipeline option: ${pipeline}`)
    }

    console.log('\n🎉 Ingestion completed successfully!')
  } catch (error) {
    console.error('❌ Ingestion failed:', error)
    throw error
  }
}
