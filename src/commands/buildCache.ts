/**
 * Build Cache Module
 *
 * Handles building the document cache by migrating documents from Couchbase
 */

import cliProgress from 'cli-progress'
import { getClient } from '../lib/couchbase.js'
import { getDocuments, getDocumentCount } from '../lib/document-processor.js'
import { Logger } from '../lib/logger.js'

/**
 * Build cache by migrating all documents from Couchbase
 * @param options Options for cache building
 */
export async function buildCache(
  options: { skipAttachments?: boolean; debug?: boolean } = {}
): Promise<void> {
  const { skipAttachments = true, debug = false } = options
  const logger = new Logger(debug)

  let progressBar: cliProgress.SingleBar | null = null

  const client = getClient({ debug })

  try {
    await client.connect()

    logger.log('✨ Migration framework ready!')
    if (skipAttachments) {
      logger.log(
        '⏭️ Skipping binary attachments - processing JSON documents only'
      )
    }

    // Get total document count for progress bar
    logger.info('📊 Getting total document count...')
    const totalDocuments = await getDocumentCount(
      client,
      logger,
      skipAttachments
    )
    logger.info(`📊 Found ${totalDocuments} documents to process`)

    // Paginate through all documents
    if (debug) {
      logger.log('\n📄 Starting full document migration...')
    } else {
      progressBar = new cliProgress.SingleBar(
        {
          format:
            '📄 Building cache |{bar}| {percentage}% | {value}/{total} documents | Elapsed: {duration_formatted} | ETA: {eta_formatted}',
          barCompleteChar: '\u2588',
          barIncompleteChar: '\u2591',
          hideCursor: true,
        },
        cliProgress.Presets.shades_classic
      )
      progressBar.start(totalDocuments, 0)
    }

    let offset = 0
    const limit = 1000
    let totalProcessed = 0
    let totalSkipped = 0
    let pageNumber = 1

    while (true) {
      logger.log(`\n📄 Processing page ${pageNumber} (offset: ${offset})...`)
      const paginationResult = await getDocuments(client, logger, {
        offset,
        limit,
        skipAttachments,
      })

      const documentsInPage =
        paginationResult.documentsProcessed + paginationResult.documentsSkipped
      totalProcessed += paginationResult.documentsProcessed
      totalSkipped += paginationResult.documentsSkipped

      const total = totalProcessed + totalSkipped

      if (progressBar) {
        progressBar.update(total, {
          value: total,
        })
      }

      logger.log(`📋 Retrieved ${documentsInPage} documents in this page`)
      logger.log(`✅ Processed: ${paginationResult.documentsProcessed}`)
      logger.log(`⏭️ Skipped: ${paginationResult.documentsSkipped}`)
      logger.log(`🔄 Has more pages: ${paginationResult.hasMore}`)

      if (documentsInPage > 0) {
        logger.log(
          `✅ Successfully processed ${paginationResult.documentsProcessed} documents in page ${pageNumber}`
        )
      } else {
        logger.log(`ℹ️ No documents found in page ${pageNumber}`)
      }

      // Update offset for next page
      offset = paginationResult.nextOffset
      pageNumber++

      // Check if we should continue
      if (!paginationResult.hasMore) {
        if (progressBar) {
          progressBar.stop()
        }
        logger.log('\n🏁 No more pages available - migration complete!')
        break
      }

      // Add a small delay between pages to be gentle on the server
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    // Final statistics
    logger.info('\n📊 Migration Summary:')
    logger.info(`📄 Total pages processed: ${pageNumber - 1}`)
    logger.info(`📋 Total documents found: ${totalProcessed + totalSkipped}`)
    logger.info(`✅ Total documents processed: ${totalProcessed}`)
    logger.info(`⏭️ Total documents skipped (already existed): ${totalSkipped}`)
  } catch (error) {
    if (progressBar) {
      progressBar.stop()
    }
    logger.error('❌ Error during Couchbase operations:', error)
    throw error
  } finally {
    try {
      await client.disconnect()
    } catch (disconnectError) {
      logger.error('❌ Error disconnecting:', disconnectError)
    }
  }
}
