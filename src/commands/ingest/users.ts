/**
 * User Ingestion Pipeline
 *
 * Handles ingesting user documents from the cache into Core
 */

import { promises as fs } from 'fs'
import path from 'path'
import { z } from 'zod'
import admin from 'firebase-admin'
import {
  prismaApiUsers,
  Prisma,
  User,
} from '../../lib/prisma/api-users/client.js'
import { prismaUsers } from '../../lib/prisma/users/client.js'
import { v4 as uuidv4 } from 'uuid'
import { auth } from '../../lib/firebase.js'

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
async function processUserFile(
  filePath: string,
  dryRun: boolean
): Promise<User | null> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const rawData = JSON.parse(fileContent)

    const userData = validateAndTransformUser(rawData)
    if (!userData) {
      console.log(`⏭️ Skipping invalid user file: ${filePath}`)
      return null
    }

    if (dryRun) {
      console.log(`⏭️ Skipping user ${userData.theKeySsoGuid} in dry run`)
      return null
    }
    // Check if user exists by email in Firebase
    let firebaseUser: admin.auth.UserRecord | null = null
    try {
      try {
        firebaseUser = await auth.getUserByEmail(userData.email)
        console.log(
          `ℹ️ User with email ${userData.email} already exists in Firebase (UID: ${firebaseUser.uid})`
        )
        const oktaProvider = firebaseUser.providerData.find(
          provider => provider.providerId === 'oidc.okta'
        )
        if (!oktaProvider) {
          firebaseUser = await auth.updateUser(firebaseUser.uid, {
            providerToLink: {
              providerId: 'oidc.okta',
              uid: userData.theKeySsoGuid,
              displayName: `${userData.nameFirst} ${userData.nameLast}`.trim(),
              email: userData.email,
            },
          })
          console.log(
            `✅ Updated Firebase user for ${userData.email} with Okta OCID: ${userData.theKeySsoGuid}`
          )
        } else {
          console.log(`✅ User ${userData.email} already has Okta provider`)
        }
      } catch (error: unknown) {
        // User doesn't exist if error code is 'auth/user-not-found'
        const firebaseError = error as { code?: string }
        if (firebaseError.code === 'auth/user-not-found') {
          try {
            firebaseUser = await auth.createUser({
              email: userData.email,
              emailVerified: true,
              displayName: `${userData.nameFirst} ${userData.nameLast}`.trim(),
              disabled: false,
            })
            //  link Okta provider
            firebaseUser = await auth.updateUser(firebaseUser.uid, {
              providerToLink: {
                providerId: 'oidc.okta',
                uid: userData.theKeySsoGuid,
                displayName:
                  `${userData.nameFirst} ${userData.nameLast}`.trim(),
                email: userData.email,
              },
            })
            console.log(
              `✅ Created Firebase user for ${userData.email} with Okta OCID: ${userData.theKeySsoGuid}`
            )
          } catch (error) {
            console.error(
              `❌ Error creating Firebase user for ${userData.email}:`,
              error
            )
            return null
          }
        } else {
          // Some other error occurred
          throw error
        }
      }
    } catch (error) {
      console.error(
        `❌ Error uploading user to firebase for user file ${filePath}:`,
        error
      )
      return null
    }

    // Save to database using Prisma
    try {
      if (!firebaseUser || !firebaseUser.email) {
        console.error(`❌ Firebase user not found for user ${userData.email}`)
        return null
      }

      const user: Prisma.UserCreateInput = {
        id: uuidv4(),
        userId: firebaseUser.uid,
        firstName: firebaseUser.displayName?.split(' ')[0] ?? '',
        lastName: firebaseUser.displayName?.split(' ')[1] ?? '',
        email: firebaseUser.email,
        emailVerified: firebaseUser.emailVerified,
        superAdmin: false,
      }

      const userSavedToCore = await prismaApiUsers.user.upsert({
        where: { id: userData.owner },
        update: user,
        create: user,
      })

      console.log(`✅ Saved user ${firebaseUser.email} to database`)
      const userToSaveToLocal = {
        ownerId: userData.owner,
        email: firebaseUser.email,
        ssoGuid: userData.theKeySsoGuid,
        coreId: userSavedToCore.id,
      }
      await prismaUsers.user.upsert({
        where: { email: firebaseUser.email },
        update: userToSaveToLocal,
        create: userToSaveToLocal,
      })
      console.log(`✅ Saved user ${firebaseUser.email} to local database`)
      return userSavedToCore
    } catch (dbError) {
      console.error(`❌ Database error for user ${userData.owner}:`, dbError)
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
  const processedUsers: User[] = []
  let successCount = 0
  let errorCount = 0

  for (const filePath of userFiles) {
    const processedUser = await processUserFile(filePath, dryRun)
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
      console.log(`  Name: ${user.firstName} ${user.lastName ?? ''}`)
      console.log(`  Email: ${user.email}`)
      console.log(`  User ID: ${user.userId}`)
    })
  }
}
