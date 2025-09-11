/**
 * Document Processor
 *
 * Handles processing of individual documents from Couchbase
 */

import { promises as fs } from 'fs'
import path from 'path'

export interface Document {
  id: string
  content: Buffer
  cas: string
}

/**
 * Process a single document asynchronously
 * @param document Document to process
 * @returns Promise that resolves when processing is complete
 */
export async function processDocument(document: Document): Promise<void> {
  try {
    console.log(`🔄 Processing document: ${document.id}`)

    // Ensure output directory exists
    await fs.mkdir('./tmp', { recursive: true })

    // Create a safe filename from the document ID
    const safeFilename = document.id.replace(/[^a-zA-Z0-9._-]/g, '_')
    const filePath = path.join('./tmp', `${safeFilename}.bin`)

    // Write buffer content to file
    await fs.writeFile(filePath, document.content)

    console.log(`📁 Written document ${document.id} to: ${filePath}`)
    console.log(`📊 File size: ${document.content.length} bytes`)

    console.log(`✅ Successfully processed document: ${document.id}`)
  } catch (error) {
    console.error(`❌ Error processing document ${document.id}:`, error)
    throw error
  }
}
