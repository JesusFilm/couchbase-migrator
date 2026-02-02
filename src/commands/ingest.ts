/**
 * Ingest Module
 *
 * Handles ingesting documents from the cache into Core
 */

import { ingestUsers, type UserIngestionSummary } from './ingest/users.js'
import {
  ingestPlaylists,
  type PlaylistIngestionSummary,
} from './ingest/playlists.js'

export interface IngestOptions {
  sourceDir?: string
  dryRun?: boolean
  pipeline?: 'users' | 'playlists' | 'all'
  file?: string
  concurrency?: number
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

  let userSummary: UserIngestionSummary | null = null
  let playlistSummary: PlaylistIngestionSummary | null = null

  try {
    switch (pipeline) {
      case 'users':
        userSummary = await ingestUsers({
          sourceDir,
          dryRun,
          ...(options.file && { file: options.file }),
        })
        break

      case 'playlists':
        playlistSummary = await ingestPlaylists({
          sourceDir,
          dryRun,
          ...(options.file && { file: options.file }),
        })
        break

      case 'all':
        if (options.file) {
          throw new Error(
            '--file option can only be used with --pipeline users or --pipeline playlists, not --pipeline all'
          )
        }
        userSummary = await ingestUsers({
          sourceDir,
          dryRun,
          ...(options.concurrency && { concurrency: options.concurrency }),
        })
        playlistSummary = await ingestPlaylists({ sourceDir, dryRun })
        break

      default:
        throw new Error(`Invalid pipeline option: ${pipeline}`)
    }

    // Log summaries at the end
    console.log('\n' + '='.repeat(60))
    console.log('📊 INGESTION SUMMARY')
    console.log('='.repeat(60))

    if (userSummary) {
      console.log('\n📈 User Ingestion Summary:')
      console.log(
        `✅ Successfully processed: ${userSummary.successCount} users`
      )
      console.log(`❌ Failed to process: ${userSummary.errorCount} users`)
      console.log(`📊 Total files: ${userSummary.totalFiles}`)
    }

    if (playlistSummary) {
      console.log('\n📈 Playlist Ingestion Summary:')
      const batchLine = [
        `✅ Batch write: ${playlistSummary.successCount} playlists`,
        `${playlistSummary.analysis.totalSavedItems} items saved`,
        `${playlistSummary.analysis.totalSkippedItems} items skipped`,
        `${playlistSummary.analysis.totalItemsNotProcessed} items not processed (owner missing)`,
        `${playlistSummary.videoVariantsNotFound} VideoVariants not found`,
      ]
      console.log(batchLine.join(', '))
      console.log(
        `✅ Successfully processed: ${playlistSummary.successCount} playlists`
      )
      console.log(
        `❌ Failed to process: ${playlistSummary.errorCount} playlists`
      )
      console.log(`📊 Total files: ${playlistSummary.totalFiles}`)
      console.log(
        `🎵 Total playlist items: ${playlistSummary.analysis.totalItems}`
      )
      console.log(
        `✅ Successfully saved playlist items: ${playlistSummary.analysis.totalSavedItems}`
      )
      console.log(
        `❌ Skipped playlist items (VideoVariant not found): ${playlistSummary.analysis.totalSkippedItems}`
      )
      console.log(
        `❌ Skipped playlist items (playlist owner missing): ${playlistSummary.analysis.totalItemsNotProcessed}`
      )
      console.log(
        `📺 VideoVariants not found: ${playlistSummary.videoVariantsNotFound}`
      )
      const itemsSum =
        playlistSummary.analysis.totalSavedItems +
        playlistSummary.analysis.totalSkippedItems +
        playlistSummary.analysis.totalItemsNotProcessed
      if (itemsSum !== playlistSummary.analysis.totalItems) {
        console.warn(
          `⚠️ Item count mismatch: total ${playlistSummary.analysis.totalItems} ≠ saved + skipped + not processed (${itemsSum})`
        )
      }
      console.log(
        `📺 Unique media components: ${playlistSummary.analysis.uniqueMediaComponents.size}`
      )
      console.log(
        `📊 Average items per playlist: ${playlistSummary.analysis.averageItemsPerPlaylist.toFixed(2)}`
      )

      // Language distribution
      if (playlistSummary.analysis.languageDistribution.size > 0) {
        console.log('\n🌍 Language Distribution:')
        const sortedLanguages = Array.from(
          playlistSummary.analysis.languageDistribution.entries()
        )
          .sort((a, b) => b[1] - a[1])
          .slice(0, 5) // Show top 5 languages

        for (const [languageId, count] of sortedLanguages) {
          console.log(`  Language ${languageId}: ${count} items`)
        }
      }
    }

    console.log('\n🎉 Ingestion completed successfully!')
  } catch (error) {
    console.error('❌ Ingestion failed:', error)
    throw error
  }
}
