/**
 * Document Processor
 *
 * Handles processing of individual documents from Couchbase
 */

import { promises as fs } from 'fs'
import path from 'path'
import { fileTypeFromBuffer } from 'file-type'
import { CouchbaseClient } from './couchbase'

export interface Document {
  id: string
  content: Buffer
  cas: string
}

/**
 * Detect file type from binary buffer and return appropriate extension
 * @param buffer Binary data buffer
 * @returns File extension (including dot) or '.bin' as fallback
 */
async function detectFileExtension(buffer: Buffer): Promise<string> {
  try {
    const fileType = await fileTypeFromBuffer(buffer)
    if (fileType) {
      return `.${fileType.ext}`
    }
  } catch (error) {
    console.warn('⚠️ Could not detect file type:', error)
  }

  // Fallback to .bin if detection fails
  return '.bin'
}

/**
 * Retry a function with exponential backoff
 * @param fn Function to retry
 * @param maxRetries Maximum number of retries (default: 3)
 * @param baseDelay Base delay in milliseconds (default: 1000)
 * @returns Promise that resolves with the function result
 */
async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  baseDelay: number = 1000
): Promise<T> {
  let lastError: Error | undefined

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn()
    } catch (error) {
      lastError = error as Error

      if (attempt === maxRetries) {
        console.error(
          `❌ Failed after ${maxRetries} attempts:`,
          lastError.message
        )
        throw lastError
      }

      // Calculate delay with exponential backoff: baseDelay * 2^(attempt-1)
      const delay = baseDelay * Math.pow(2, attempt - 1)
      console.warn(
        `⚠️ Attempt ${attempt}/${maxRetries} failed, retrying in ${delay}ms: ${lastError.message}`
      )

      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }

  throw lastError || new Error('Unknown error occurred')
}

/**
 * Paginate binary documents in the collection using N1QL for IDs, then KV for content
 * Processes documents immediately as they're retrieved instead of collecting in an array
 * @param client Couchbase client instance
 * @param options Pagination options
 * @returns Promise containing pagination info and processing statistics
 */
export async function getDocuments(
  client: CouchbaseClient,
  options?: { offset?: number; limit?: number }
): Promise<{
  documentsProcessed: number
  documentsSkipped: number
  hasMore: boolean
  nextOffset: number
}> {
  const { offset = 0, limit = 10 } = options ?? {}

  // Get required objects from the client
  const cluster = await client.getCluster()
  const bucket = await client.getBucket()
  const config = client.getConfig()
  const collection = bucket.defaultCollection()

  // First, get document IDs using N1QL (this works fine)
  const query = `
      SELECT META().id as id
      FROM \`${config.bucketName}\`
      LIMIT $LIMIT OFFSET $OFFSET
    `

  const result = await withRetry(
    () =>
      cluster.query<{ id: string }>(query, {
        parameters: {
          LIMIT: limit + 1,
          OFFSET: offset,
        },
      }),
    3, // max retries
    500 // base delay in ms (shorter for queries)
  )

  const hasMore = result.rows.length > limit
  const documentIds = result.rows.slice(0, limit)
  const nextOffset = hasMore ? offset + limit : offset

  let documentsProcessed = 0
  let documentsSkipped = 0

  for (const { id } of documentIds) {
    try {
      // Check if file already exists in temp directory
      const safeFilename = id.replace(/[^a-zA-Z0-9._-]/g, '_')
      const tempDir = './tmp'

      // Ensure temp directory exists
      await fs.mkdir(tempDir, { recursive: true })

      // Check for existing files with common extensions
      const possibleExtensions = [
        '.jpg',
        '.jpeg',
        '.png',
        '.gif',
        '.pdf',
        '.bin',
      ]
      let existingFile = null

      for (const ext of possibleExtensions) {
        const filePath = path.join(tempDir, `${safeFilename}${ext}`)
        try {
          await fs.access(filePath)
          existingFile = filePath
          break
        } catch {
          // File doesn't exist with this extension, continue
        }
      }

      if (existingFile) {
        console.log(
          `⏭️ Skipping document ${id} - already exists: ${existingFile}`
        )
        documentsSkipped++
        continue
      }

      // Fetch the document
      const doc = await withRetry(
        () =>
          collection.get(id, {
            timeout: config.operationTimeout,
          }),
        3, // max retries
        1000 // base delay in ms
      )

      console.log(
        `✅ Fetched document: ${id} (${(doc.content as Buffer).length} bytes)`
      )

      // Process the document immediately
      const document: Document = {
        id,
        content: doc.content as Buffer,
        cas: doc.cas.toString(),
      }

      await processDocument(document)
      documentsProcessed++
    } catch (error) {
      console.error(`❌ Error processing document ${id}:`, error)
      // Continue with other documents even if one fails
    }
  }

  return {
    documentsProcessed,
    documentsSkipped,
    hasMore,
    nextOffset,
  }
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

    // Detect file type and get appropriate extension
    const fileExtension = await detectFileExtension(document.content)

    // Create a safe filename from the document ID
    const safeFilename = document.id.replace(/[^a-zA-Z0-9._-]/g, '_')
    const filePath = path.join('./tmp', `${safeFilename}${fileExtension}`)

    // Write buffer content to file
    await fs.writeFile(filePath, document.content)

    console.log(`📁 Written document ${document.id} to: ${filePath}`)
    console.log(`📊 File size: ${document.content.length} bytes`)
    console.log(`🔍 Detected file type: ${fileExtension}`)

    console.log(`✅ Successfully processed document: ${document.id}`)
  } catch (error) {
    console.error(`❌ Error processing document ${document.id}:`, error)
    throw error
  }
}
