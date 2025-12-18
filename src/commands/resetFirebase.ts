import { promises as fs } from 'fs'
import path from 'path'
import { auth } from '../lib/firebase.js'
import { writeErrorToFile, clearErrorsDirectory } from '../lib/error-handler.js'

export async function resetFirebase(
  sourceDir: string = './tmp'
): Promise<void> {
  // Clear errors directory at the beginning
  await clearErrorsDirectory(sourceDir, 'firebaseDelete')

  const userDir = path.join(sourceDir, 'u')
  const emails = new Set<string>()

  // Extract emails from cache
  const files = await fs.readdir(userDir)
  for (const file of files.filter(f => f.endsWith('.json'))) {
    try {
      const data = JSON.parse(
        await fs.readFile(path.join(userDir, file), 'utf-8')
      )
      if (data['JFM-profiles']?.email) {
        emails.add(data['JFM-profiles'].email.toLowerCase().trim())
      }
    } catch {
      // Skip invalid files
    }
  }

  console.log(`Found ${emails.size} unique emails`)

  // Get UIDs in batches of 100 (Firebase limit)
  const uids: string[] = []
  const emailArray = Array.from(emails)
  let processed = 0
  let notFound = 0
  let errors = 0
  const BATCH_SIZE = 100

  for (let i = 0; i < emailArray.length; i += BATCH_SIZE) {
    const emailBatch = emailArray.slice(i, i + BATCH_SIZE)
    const emailIdentifiers = emailBatch.map(email => ({ email }))

    try {
      const result = await auth.getUsers(emailIdentifiers)

      // Process found users
      for (const user of result.users) {
        uids.push(user.uid)
        processed++
      }

      // Track not found users
      notFound += result.notFound.length

      // Log progress
      const totalProcessed = processed + notFound + errors
      console.log(
        `  Processed ${totalProcessed}/${emailArray.length} emails (${processed} found, ${notFound} not found, ${errors} errors)`
      )
    } catch {
      // If batch fails, try individual lookups to identify problematic emails
      console.warn(
        `  ⚠️  Batch lookup failed, trying individual lookups for ${emailBatch.length} emails`
      )

      for (const email of emailBatch) {
        try {
          const user = await auth.getUserByEmail(email)
          uids.push(user.uid)
          processed++
        } catch (individualError) {
          const firebaseError = individualError as { code?: string }
          if (firebaseError.code === 'auth/user-not-found') {
            notFound++
          } else {
            // Only save actual errors (format issues, etc.)
            errors++
            const errorFilePath = path.join(
              sourceDir,
              'u',
              `${email.replace(/[^a-zA-Z0-9]/g, '_')}.json`
            )
            await writeErrorToFile(
              sourceDir,
              'firebaseDelete',
              errorFilePath,
              individualError,
              { email }
            )
          }
        }
      }

      // Log progress after individual lookups
      const totalProcessed = processed + notFound + errors
      console.log(
        `  Processed ${totalProcessed}/${emailArray.length} emails (${processed} found, ${notFound} not found, ${errors} errors)`
      )
    }
  }

  console.log(
    `Found ${uids.length} users in Firebase (${notFound} not found, ${errors} errors)`
  )

  // Delete in batches of 1000
  for (let i = 0; i < uids.length; i += 1000) {
    const batch = uids.slice(i, i + 1000)
    const result = await auth.deleteUsers(batch)
    console.log(
      `Deleted ${result.successCount} users (batch ${Math.floor(i / 1000) + 1})`
    )
  }

  console.log('Done!')
}
