/**
 * Error Handler Utility
 *
 * Handles writing errors to files and managing error directories
 */

import { promises as fs } from 'fs'
import path from 'path'

/**
 * Write error to errors directory
 * @param sourceDir Base source directory
 * @param category Error category (e.g., 'users', 'playlists')
 * @param filePath Original file path
 * @param error Error that occurred
 */
export async function writeErrorToFile(
  sourceDir: string,
  category: string,
  filePath: string,
  error: unknown
): Promise<void> {
  try {
    const errorsDir = path.join(sourceDir, 'errors', category)
    await fs.mkdir(errorsDir, { recursive: true })

    // Extract filename from filePath
    const filename = path.basename(filePath)
    const errorFilePath = path.join(errorsDir, filename)

    // Convert error to string
    const errorString =
      error instanceof Error
        ? error.message
        : typeof error === 'string'
          ? error
          : JSON.stringify(error, null, 2)

    await fs.writeFile(errorFilePath, errorString, 'utf8')
  } catch (writeError) {
    console.error(`❌ Failed to write error file for ${filePath}:`, writeError)
  }
}

/**
 * Clear errors directory for a specific category
 * @param sourceDir Base source directory
 * @param category Error category (e.g., 'users', 'playlists')
 */
export async function clearErrorsDirectory(
  sourceDir: string,
  category: string
): Promise<void> {
  try {
    const errorsDir = path.join(sourceDir, 'errors', category)
    // Check if directory exists
    try {
      await fs.access(errorsDir)
      // Directory exists, remove all files
      const files = await fs.readdir(errorsDir)
      for (const file of files) {
        await fs.unlink(path.join(errorsDir, file))
      }
      console.log(
        `🧹 Cleared ${files.length} error file(s) from errors/${category}/`
      )
    } catch {
      // Directory doesn't exist, create it
      await fs.mkdir(errorsDir, { recursive: true })
    }
  } catch (error) {
    console.warn(`⚠️ Could not clear errors directory:`, error)
  }
}
