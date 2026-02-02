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
import { Logger } from '../lib/logger.js'

export interface IngestOptions {
  sourceDir?: string
  dryRun?: boolean
  pipeline?: 'users' | 'playlists' | 'all'
  file?: string
  concurrency?: number
  debug?: boolean
}

/**
 * Ingest documents from cache into Core
 * @param options Options for ingestion
 */
export async function ingest(options: IngestOptions = {}): Promise<void> {
  const {
    sourceDir = './tmp',
    dryRun = false,
    pipeline = 'all',
    debug = false,
  } = options
  const logger = new Logger(debug)

  logger.info('🚀 Starting document ingestion...')
  logger.info(`📁 Source directory: ${sourceDir}`)
  logger.info(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)
  logger.info(`🎯 Pipeline: ${pipeline}`)

  let userSummary: UserIngestionSummary | null = null
  let playlistSummary: PlaylistIngestionSummary | null = null

  try {
    switch (pipeline) {
      case 'users':
        userSummary = await ingestUsers({
          sourceDir,
          dryRun,
          ...(options.file && { file: options.file }),
          ...(options.concurrency && { concurrency: options.concurrency }),
          ...(options.debug !== undefined && { debug: options.debug }),
        })
        break

      case 'playlists':
        playlistSummary = await ingestPlaylists({
          sourceDir,
          dryRun,
          ...(options.file && { file: options.file }),
          ...(options.concurrency && { concurrency: options.concurrency }),
          ...(options.debug !== undefined && { debug: options.debug }),
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
          ...(options.debug !== undefined && { debug: options.debug }),
        })
        playlistSummary = await ingestPlaylists({
          sourceDir,
          dryRun,
          ...(options.concurrency && { concurrency: options.concurrency }),
          ...(options.debug !== undefined && { debug: options.debug }),
        })
        break

      default:
        throw new Error(`Invalid pipeline option: ${pipeline}`)
    }

    // Log summaries at the end
    logger.info('\n' + '='.repeat(60))
    logger.info('📊 INGESTION SUMMARY')
    logger.info('='.repeat(60))

    if (userSummary) {
      logger.info('\n📈 User Ingestion Summary:')
      logger.info(
        `✅ Successfully processed: ${userSummary.successCount} users`
      )
      logger.info(`❌ Failed to process: ${userSummary.errorCount} users`)
      logger.info(`📊 Total files: ${userSummary.totalFiles}`)
    }

    if (playlistSummary) {
      logger.info('\n📈 Playlist Ingestion Summary:')
      const batchLine = [
        `✅ Batch write: ${playlistSummary.successCount} playlists`,
        `${playlistSummary.analysis.totalSavedItems} items saved`,
        `${playlistSummary.analysis.totalSkippedItems} items skipped`,
        `${playlistSummary.analysis.totalItemsNotProcessed} items not processed (owner missing)`,
        `${playlistSummary.videoVariantsNotFound} VideoVariants not found`,
      ]
      logger.info(batchLine.join(', '))
      logger.info(
        `✅ Successfully processed: ${playlistSummary.successCount} playlists`
      )
      logger.info(
        `❌ Failed to process: ${playlistSummary.errorCount} playlists`
      )
      logger.info(`📊 Total files: ${playlistSummary.totalFiles}`)
      logger.info(
        `🎵 Total playlist items: ${playlistSummary.analysis.totalItems}`
      )
      logger.info(
        `✅ Successfully saved playlist items: ${playlistSummary.analysis.totalSavedItems}`
      )
      logger.info(
        `❌ Skipped playlist items (VideoVariant not found): ${playlistSummary.analysis.totalSkippedItems}`
      )
      logger.info(
        `❌ Skipped playlist items (playlist owner missing): ${playlistSummary.analysis.totalItemsNotProcessed}`
      )
      logger.info(
        `📺 VideoVariants not found: ${playlistSummary.videoVariantsNotFound}`
      )
      const itemsSum =
        playlistSummary.analysis.totalSavedItems +
        playlistSummary.analysis.totalSkippedItems +
        playlistSummary.analysis.totalItemsNotProcessed
      if (itemsSum !== playlistSummary.analysis.totalItems) {
        logger.warn(
          `⚠️ Item count mismatch: total ${playlistSummary.analysis.totalItems} ≠ saved + skipped + not processed (${itemsSum})`
        )
      }
      logger.info(
        `📺 Unique media components: ${playlistSummary.analysis.uniqueMediaComponents.size}`
      )
      logger.info(
        `📊 Average items per playlist: ${playlistSummary.analysis.averageItemsPerPlaylist.toFixed(2)}`
      )

      // Language distribution
      if (playlistSummary.analysis.languageDistribution.size > 0) {
        logger.info('\n🌍 Language Distribution:')
        const sortedLanguages = Array.from(
          playlistSummary.analysis.languageDistribution.entries()
        )
          .sort((a, b) => b[1] - a[1])
          .slice(0, 5) // Show top 5 languages

        for (const [languageId, count] of sortedLanguages) {
          logger.info(`  Language ${languageId}: ${count} items`)
        }
      }
    }

    logger.info('\n🎉 Ingestion completed successfully!')
  } catch (error) {
    logger.error('❌ Ingestion failed:', error)
    throw error
  }
}
