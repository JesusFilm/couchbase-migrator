/**
 * Build Cache Module
 *
 * Handles building the document cache by migrating documents from Couchbase
 */

import { client } from '../lib/couchbase.js'
import { getDocuments } from '../lib/document-processor.js'

/**
 * Build cache by migrating all documents from Couchbase
 * @param options Options for cache building
 */
export async function buildCache(
  options: { skipAttachments?: boolean } = {}
): Promise<void> {
  try {
    const { skipAttachments = true } = options

    await client.connect()

    console.log('✨ Migration framework ready!')
    if (skipAttachments) {
      console.log(
        '⏭️ Skipping binary attachments - processing JSON documents only'
      )
    }

    // Paginate through all documents
    console.log('\n📄 Starting full document migration...')

    let offset = 0
    const limit = 1000
    let totalProcessed = 0
    let totalSkipped = 0
    let pageNumber = 1

    while (true) {
      console.log(`\n📄 Processing page ${pageNumber} (offset: ${offset})...`)
      const paginationResult = await getDocuments(client, {
        offset,
        limit,
        skipAttachments,
      })

      const documentsInPage =
        paginationResult.documentsProcessed + paginationResult.documentsSkipped
      totalProcessed += paginationResult.documentsProcessed
      totalSkipped += paginationResult.documentsSkipped

      console.log(`📋 Retrieved ${documentsInPage} documents in this page`)
      console.log(`✅ Processed: ${paginationResult.documentsProcessed}`)
      console.log(`⏭️ Skipped: ${paginationResult.documentsSkipped}`)
      console.log(`🔄 Has more pages: ${paginationResult.hasMore}`)

      if (documentsInPage > 0) {
        console.log(
          `✅ Successfully processed ${paginationResult.documentsProcessed} documents in page ${pageNumber}`
        )
      } else {
        console.log(`ℹ️ No documents found in page ${pageNumber}`)
      }

      // Update offset for next page
      offset = paginationResult.nextOffset
      pageNumber++

      // Check if we should continue
      if (!paginationResult.hasMore) {
        console.log('\n🏁 No more pages available - migration complete!')
        break
      }

      // Add a small delay between pages to be gentle on the server
      await new Promise(resolve => setTimeout(resolve, 100))
    }

    // Final statistics
    console.log('\n📊 Migration Summary:')
    console.log(`📄 Total pages processed: ${pageNumber - 1}`)
    console.log(`📋 Total documents found: ${totalProcessed + totalSkipped}`)
    console.log(`✅ Total documents processed: ${totalProcessed}`)
    console.log(`⏭️ Total documents skipped (already existed): ${totalSkipped}`)
  } catch (error) {
    console.error('❌ Error during Couchbase operations:', error)
    throw error
  } finally {
    try {
      await client.disconnect()
    } catch (disconnectError) {
      console.error('❌ Error disconnecting:', disconnectError)
    }
  }
}
