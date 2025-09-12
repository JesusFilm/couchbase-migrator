/**
 * User Ingestion Pipeline
 *
 * Handles ingesting user documents from the cache into Core
 */

import { promises as fs } from 'fs'
import path from 'path'
import { z } from 'zod'
import { prisma } from '../../lib/prisma.js'
import { Prisma } from '.prisma/client'

// Zod schemas for user data validation
const SyncDataSchema = z.object({
  rev: z.string(),
  sequence: z.number(),
  recent_sequences: z.array(z.number()),
  history: z.object({
    revs: z.array(z.string()),
    parents: z.array(z.number()),
    channels: z.array(z.union([z.null(), z.array(z.string())])),
  }),
  channels: z.record(z.string(), z.union([z.null(), z.object({})])).optional(),
  access: z.record(z.string(), z.record(z.string(), z.number())).optional(),
  time_saved: z.string(),
})

const UserProfileSchema = z.object({
  _sync: SyncDataSchema,
  createdAt: z.string(),
  email: z.email(),
  homeCountry: z.string().optional(),
  nameFirst: z.string(),
  nameLast: z.string(),
  notificationCountries: z.array(z.string()).optional().default([]),
  owner: z.string(),
  theKeyGrPersonId: z.string().optional().nullable(),
  theKeyGuid: z.string(),
  theKeyRelayGuid: z.string(),
  theKeySsoGuid: z.string(),
  type: z.literal('profile'),
  updatedAt: z.string(),
})

type UserProfile = z.infer<typeof UserProfileSchema> & { cas: number }

const UserDocumentSchema = z.object({
  'JFM-profiles': UserProfileSchema,
  cas: z.number(),
})

// Hardcoded list of cas values to skip
const SKIP_CAS: number[] = [
  // Add more cas values to skip as needed
  1566300870055755800, 1687279660005064700, 1673036801239613400,
  1593720804749148200, 1672946638568226800,
]

/**
 * Validate and transform user document data using Zod
 * @param rawData Raw JSON data from file
 * @returns Processed user data for Prisma or null if invalid
 */
function validateAndTransformUser(rawData: unknown): UserProfile | null {
  try {
    // Check if user should be skipped based on cas BEFORE parsing
    let cas: number | undefined
    if (
      rawData &&
      typeof rawData === 'object' &&
      'cas' in rawData &&
      typeof rawData.cas === 'number'
    ) {
      cas = rawData.cas
    }

    if (cas && SKIP_CAS.includes(cas)) {
      return null
    }

    // Parse and validate the raw data with Zod
    const parseResult = UserDocumentSchema.safeParse(rawData)

    if (!parseResult.success) {
      console.warn(
        '⚠️ User document validation failed:',
        parseResult.error.issues,
        cas
      )
      return null
    }

    return {
      ...parseResult.data['JFM-profiles'],
      cas: parseResult.data.cas,
    }
  } catch (error) {
    console.error('❌ Error validating user data:', error)
    return null
  }
}

/**
 * Process a single user JSON file
 * @param filePath Path to the user JSON file
 * @returns Processed user data or null if processing failed
 */
async function processUserFile(filePath: string): Promise<any | null> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const rawData = JSON.parse(fileContent)

    const userData = validateAndTransformUser(rawData)
    if (!userData) {
      return null
    }

    // Save to database using Prisma
    try {
      const user: Prisma.UserCreateInput = {
        ...userData,
        id: userData.theKeySsoGuid,
        syncRev: userData._sync.rev,
        syncSequence: userData._sync.sequence,
        syncRecentSequences: userData._sync.recent_sequences.join(','),
        syncTimeSaved: userData._sync.time_saved,
        theKeyGrPersonId: userData.theKeyGrPersonId || null,
        homeCountry: userData.homeCountry || null,
        notificationCountries: userData.notificationCountries.join(','),
        ingestedAt: new Date(),
        cas: BigInt(userData.cas),
      }

      const savedUser = await prisma.user.upsert({
        where: { theKeySsoGuid: userData.theKeySsoGuid },
        update: user,
        create: user,
      })

      return savedUser
    } catch (dbError) {
      console.error(
        `❌ Database error for user ${userData.theKeySsoGuid}:`,
        dbError
      )
      return null
    }
  } catch (error) {
    console.error(`❌ Error processing user file ${filePath}:`, error)
    return null
  }
}

/**
 * Get all user JSON files from both user directories
 * @param sourceDir Base source directory
 * @returns Array of file paths
 */
async function getUserFiles(sourceDir: string): Promise<string[]> {
  const userDirs = ['user', 'u']
  const allFiles: string[] = []

  for (const userDir of userDirs) {
    const fullPath = path.join(sourceDir, userDir)
    try {
      const files = await fs.readdir(fullPath)
      const jsonFiles = files
        .filter(file => file.endsWith('.json'))
        .map(file => path.join(fullPath, file))
      allFiles.push(...jsonFiles)
    } catch (error) {
      console.warn(`⚠️ Could not read directory ${fullPath}:`, error)
      // Continue with other directories even if one fails
    }
  }

  return allFiles
}

/**
 * Ingest users from cache directory
 * @param options Options for user ingestion
 */
export async function ingestUsers(
  options: { sourceDir?: string; dryRun?: boolean } = {}
): Promise<void> {
  const { sourceDir = './tmp', dryRun = false } = options

  console.log('👥 Starting user ingestion pipeline...')
  console.log(`📁 Source directory: ${sourceDir}`)
  console.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)

  // Get all user files from both user and u directories
  const userFiles = await getUserFiles(sourceDir)
  if (userFiles.length === 0) {
    console.log('ℹ️ No user files found in user/ or u/ directories')
    return
  }

  console.log(`📊 Found ${userFiles.length} user files to process`)

  // Process each user file
  const processedUsers: any[] = []
  let successCount = 0
  let errorCount = 0

  for (const filePath of userFiles) {
    const processedUser = await processUserFile(filePath)
    if (processedUser) {
      processedUsers.push(processedUser)
      successCount++
    } else {
      errorCount++
    }
  }

  // Summary
  console.log('\n📈 User Ingestion Summary:')
  console.log(`✅ Successfully processed: ${successCount} users`)
  console.log(`❌ Failed to process: ${errorCount} users`)
  console.log(`📊 Total files: ${userFiles.length}`)

  if (dryRun) {
    console.log('\n🔍 Dry run - showing sample processed users:')
    processedUsers.slice(0, 3).forEach((user, index) => {
      console.log(`\nUser ${index + 1}:`)
      console.log(`  Name: ${user.nameFirst} ${user.nameLast}`)
      console.log(`  Email: ${user.email}`)
      console.log(`  Country: ${user.homeCountry}`)
      console.log(`  SSO GUID: ${user.theKeySsoGuid}`)
    })
  }
}
