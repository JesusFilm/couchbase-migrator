#!/usr/bin/env node

/**
 * Couchbase Migrator - Main Entry Point
 *
 * This is the main entry point for the Couchbase migration tool.
 */

import { client } from '@/lib/couchbase'
import { processDocument } from '@/lib/document-processor'

// Main execution
export async function main(): Promise<void> {
  try {
    await client.connect()

    console.log('✨ Migration framework ready!')

    // Demonstrate pagination of binary documents
    console.log('\n📄 getting documents...')
    const paginationResult = await client.getDocuments()

    console.log(
      `📋 Retrieved ${paginationResult.documents.length} documents in this page`
    )
    console.log(`🔄 Has more pages: ${paginationResult.hasMore}`)

    if (paginationResult.documents.length > 0) {
      console.log('\n📄 Processing documents...')

      // Process each document asynchronously and await completion
      for (const document of paginationResult.documents) {
        await processDocument(document)
      }

      console.log(
        `\n✅ Successfully processed all ${paginationResult.documents.length} documents`
      )
    } else {
      console.log('ℹ️ No documents found in the collection')
    }
  } catch (error) {
    console.error('❌ Error during Couchbase operations:', error)
  } finally {
    try {
      await client.disconnect()
    } catch (disconnectError) {
      console.error('❌ Error disconnecting:', disconnectError)
    }
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Fatal error:', error)
    process.exit(1)
  })
}
