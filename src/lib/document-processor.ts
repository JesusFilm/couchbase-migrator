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
 * Generate folder path based on document ID structure
 * @param id Document ID
 * @returns Folder path (e.g., "_sync/att" for "_sync:att:sha1-...", "pl" for "pl_...", undefined for simple IDs)
 */
function generateFolderPath(id: string): string | undefined {
  // Handle underscore-prefixed IDs (pl_, mc_, u_, user_)
  if (id.startsWith('pl_')) {
    return 'pl'
  }
  if (id.startsWith('mc_')) {
    return 'mc'
  }
  if (id.startsWith('u_')) {
    return 'u'
  }
  if (id.startsWith('user_')) {
    return 'user'
  }

  // Split by colon to get the parts
  const parts = id.split(':')

  if (parts.length >= 2) {
    // For IDs like "_sync:att:sha1-..." -> "_sync/att"
    // For IDs like "user:123" -> "user"
    const folderParts = parts.slice(0, 2)
    return folderParts.join('/')
  }

  return undefined
}

/**
 * Generate filename based on document ID structure
 * @param id Document ID
 * @returns Filename (e.g., "sha1-abc123" for "_sync:att:sha1-abc123", "123" for "pl_123", full ID for simple IDs)
 */
function generateFilename(id: string): string {
  // Handle underscore-prefixed IDs (pl_, mc_, u_, user_)
  if (id.startsWith('pl_')) {
    return id.substring(3) // Remove "pl_" prefix
  }
  if (id.startsWith('mc_')) {
    return id.substring(3) // Remove "mc_" prefix
  }
  if (id.startsWith('u_')) {
    return id.substring(2) // Remove "u_" prefix
  }
  if (id.startsWith('user_')) {
    return id.substring(5) // Remove "user_" prefix
  }

  // Split by colon to get the parts
  const parts = id.split(':')

  if (parts.length >= 2) {
    // For IDs like "_sync:att:sha1-abc123" -> "sha1-abc123"
    // For IDs like "user:123" -> "123"
    const lastPart = parts[parts.length - 1]
    return lastPart || id // Fallback to full ID if last part is empty
  }

  // For simple IDs without colons, use the full ID
  return id
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
  const config = client.getConfig()

  // Get documents with both metadata and content using N1QL
  const query = `
      SELECT META().id as id, META().cas as cas, * 
      FROM \`${config.bucketName}\`
      LIMIT $LIMIT OFFSET $OFFSET
    `

  const result = await withRetry(
    () =>
      cluster.query<{ id: string; cas: string; [key: string]: unknown }>(
        query,
        {
          timeout: config.operationTimeout,
          parameters: {
            LIMIT: limit + 1,
            OFFSET: offset,
          },
        }
      ),
    3, // max retries
    500
  )

  const hasMore = result.rows.length > limit
  const documents = result.rows.slice(0, limit)
  const nextOffset = hasMore ? offset + limit : offset

  let documentsProcessed = 0
  let documentsSkipped = 0

  for (const doc of documents) {
    const { id, ...content } = doc
    try {
      // Check if this is a binary attachment (starts with _sync:att:)
      const isAttachment =
        id.startsWith('_sync:att:') || id.startsWith('_sync:rev:')
      if (isAttachment) {
        // Handle binary attachments
        const wasProcessed = await processAttachment(id, client)
        if (wasProcessed) {
          documentsProcessed++
        } else {
          documentsSkipped++
        }
      } else {
        // Handle JSON documents
        const wasProcessed = await processJsonDocument(id, content)
        if (wasProcessed) {
          documentsProcessed++
        } else {
          documentsSkipped++
        }
      }
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
 * Process a single attachment asynchronously
 * @param id Attachment document ID
 * @param client Couchbase client instance
 * @returns Promise that resolves to true if processed, false if skipped
 */
export async function processAttachment(
  id: string,
  client: CouchbaseClient
): Promise<boolean> {
  try {
    const config = client.getConfig()
    const bucket = await client.getBucket()
    const collection = bucket.defaultCollection()

    // Handle binary attachments
    const filename = generateFilename(id)
    const safeFilename = filename.replace(/[^a-zA-Z0-9._-]/g, '_')
    const folderPath = generateFolderPath(id)
    const tempDir = folderPath ? path.join('./tmp', folderPath) : './tmp'

    // Ensure temp directory exists
    await fs.mkdir(tempDir, { recursive: true })

    // Check for existing files with common extensions
    const possibleExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.bin']
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
        `⏭️ Skipping attachment ${id} - already exists: ${existingFile}`
      )
      return false
    }

    // Fetch the binary attachment using collection.get
    const binaryDoc = await withRetry(
      () =>
        collection.get(id, {
          timeout: config.operationTimeout,
        }),
      3, // max retries
      1000 // base delay in ms
    )

    console.log(
      `✅ Fetched attachment: ${id} (${(binaryDoc.content as Buffer).length} bytes)`
    )

    // Process the binary document
    const document: Document = {
      id,
      content: binaryDoc.content as Buffer,
      cas: binaryDoc.cas.toString(),
    }

    console.log(`🔄 Processing attachment: ${document.id}`)

    // Detect file type and get appropriate extension
    const fileExtension = await detectFileExtension(document.content)

    // Create file path with detected extension
    const filePath = path.join(tempDir, `${safeFilename}${fileExtension}`)

    // Write buffer content to file
    await fs.writeFile(filePath, document.content)

    console.log(`📁 Written attachment ${document.id} to: ${filePath}`)
    console.log(`📊 File size: ${document.content.length} bytes`)
    console.log(`🔍 Detected file type: ${fileExtension}`)

    console.log(`✅ Successfully processed attachment: ${document.id}`)
    return true
  } catch (error) {
    console.error(`❌ Error processing attachment ${id}:`, error)
    throw error
  }
}

/**
 * Process a single JSON document asynchronously
 * @param id Document ID
 * @param content Document content (JSON object)
 * @returns Promise that resolves to true if processed, false if skipped
 */
export async function processJsonDocument(
  id: string,
  content: Record<string, unknown>
): Promise<boolean> {
  try {
    console.log(`📄 Processing JSON document: ${id}`)

    // Create a JSON file for the document
    const filename = generateFilename(id)
    const safeFilename = filename.replace(/[^a-zA-Z0-9._-]/g, '_')
    const folderPath = generateFolderPath(id)
    const tempDir = folderPath ? path.join('./tmp', folderPath) : './tmp'
    await fs.mkdir(tempDir, { recursive: true })

    const jsonFilePath = path.join(tempDir, `${safeFilename}.json`)

    // Check if JSON file already exists
    try {
      await fs.access(jsonFilePath)
      console.log(
        `⏭️ Skipping JSON document ${id} - already exists: ${jsonFilePath}`
      )
      return false
    } catch {
      // File doesn't exist, continue processing
    }

    // Write JSON document to file
    const jsonContent = JSON.stringify(content, null, 2)
    await fs.writeFile(jsonFilePath, jsonContent, 'utf8')

    console.log(`✅ Written JSON document: ${id} to ${jsonFilePath}`)
    return true
  } catch (error) {
    console.error(`❌ Error processing JSON document ${id}:`, error)
    throw error
  }
}
