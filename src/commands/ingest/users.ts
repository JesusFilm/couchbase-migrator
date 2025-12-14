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
import {
  prismaUsers,
  User as UserLocal,
} from '../../lib/prisma/users/client.js'
import { v4 as uuidv4 } from 'uuid'
import { auth } from '../../lib/firebase.js'
import {
  writeErrorToFile,
  clearErrorsDirectory,
} from '../../lib/error-handler.js'
import { env } from '../../lib/env.js'
import { UserProfileSchema, type UserProfile, type OktaUser } from './types.js'

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
function validateAndTransformUser(
  rawData: unknown,
  sourceDir: string,
  filePath: string
): UserProfile | null {
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
      writeErrorToFile(sourceDir, 'users', filePath, parseResult.error, rawData)
      return null
    }

    const userProfile = parseResult.data['JFM-profiles']

    // Normalize email to lowercase for consistent database lookups
    return {
      ...userProfile,
      email: userProfile.email.toLowerCase(),
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
 * @param sourceDir Base source directory for error files
 * @param dryRun Whether this is a dry run
 * @returns Processed user data or null if processing failed
 */
async function processUserFile(
  filePath: string,
  sourceDir: string,
  dryRun: boolean
): Promise<User | UserLocal | null> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const rawData = JSON.parse(fileContent)

    const userData = validateAndTransformUser(rawData, sourceDir, filePath)
    if (!userData) {
      console.log(`⏭️ Skipping invalid user file: ${filePath}`)
      return null
    }

    if (dryRun) {
      console.log(`⏭️ Skipping user ${userData.email} in dry run`)
      return null
    }

    // Check if user already exists in local database - ownerId is the primary key
    // First check by email (since email is unique)
    const existingLocalUser = await prismaUsers.user.findUnique({
      where: { email: userData.email },
    })
    if (existingLocalUser) {
      console.log(
        `✅ User with email ${userData.email} already exists in local database`
      )
      return existingLocalUser
    }

    // Fetch user from Okta API by SSO GUID
    let oktaUserData

    try {
      const filterExpression = `profile.theKeyGuid eq "${userData.theKeySsoGuid.trim()}"`
      const oktaResponse = await fetch(
        `https://signon.okta.com/api/v1/users?search=${encodeURIComponent(filterExpression)}`,
        {
          headers: {
            Authorization: `SSWS ${env.OKTA_TOKEN}`,
            Accept: 'application/json',
          },
        }
      )

      if (!oktaResponse.ok) {
        if (oktaResponse.status === 404) {
          console.warn(
            `⚠️ User with email ${userData.email} and ssoGuid ${userData.theKeySsoGuid} not found in Okta`
          )
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            oktaResponse.status,
            userData
          )
        } else {
          const errorText = await oktaResponse.text()
          console.error(
            `❌ Okta API error (${oktaResponse.status}) for email ${userData.email} and ssoGuid ${userData.theKeySsoGuid}: ${errorText}`
          )
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            oktaResponse.status,
            userData
          )
          return null
        }
      } else {
        // Read response body once - filter endpoint returns an array
        const responseData = (await oktaResponse.json()) as OktaUser[]

        if (!responseData || responseData.length === 0) {
          console.warn(`⚠️ No users found in Okta for email ${userData.email}`)
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            new Error(
              `No users found in Okta response for email ${userData.email}: ${JSON.stringify(responseData)}`
            ),
            userData
          )
          return null
        }

        // Get the first user from the array (should only be one for exact email match)
        const resData = responseData[0]
        if (!resData) {
          console.warn(
            `⚠️ No user data in Okta response for email ${userData.email}`
          )
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            new Error('No user data in Okta response'),
            userData
          )
          return null
        }

        const emails = resData.credentials?.emails
        const primaryEmail = resData.credentials?.emails?.find(
          email => email.type === 'PRIMARY'
        )
        if (!primaryEmail) {
          console.warn(
            `⚠️ No primary email found in Okta response for email ${userData.email}`
          )
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            new Error('No primary email found in Okta response'),
            userData
          )
          return null
        }
        oktaUserData = {
          id: resData.id,
          email: userData.email,
          firstName: resData.profile.firstName,
          lastName: resData.profile.lastName,
          status: resData.status,
          primaryEmail: primaryEmail.value,
          primaryEmailObject: primaryEmail,
          secondaryEmails: emails?.map(email => email.value),
          isSecondaryAccount: userData.email !== primaryEmail.value,
          theKeySsoGuid: resData.profile.theKeyGuid,
        }

        console.log(
          `✅ Fetched Okta user data for email ${userData.email} and ssoGuid ${userData.theKeySsoGuid}:`,
          {
            id: oktaUserData?.id,
            email: oktaUserData?.email,
            status: oktaUserData?.status,
            isSecondaryAccount: oktaUserData?.isSecondaryAccount,
            firstName: oktaUserData?.firstName,
            lastName: oktaUserData?.lastName,
            primaryEmail: oktaUserData?.primaryEmail,
            secondaryEmails: oktaUserData?.secondaryEmails,
          }
        )
      }
    } catch (error) {
      console.error(
        `❌ Error fetching user from Okta API for email ${userData.email} and ssoGuid ${userData.theKeySsoGuid}:`,
        error
      )
      await writeErrorToFile(sourceDir, 'users', filePath, error, userData)
      return null
      // Continue processing even if Okta fetch fails
    }
    // it shouhldn't be null but adding this to satisfy ts-lint
    if (!oktaUserData) {
      await writeErrorToFile(
        sourceDir,
        'users',
        filePath,
        new Error('oktaUserData object is null'),
        userData
      )
      return null
    }

    // Check if user exists by email in Firebase
    let firebaseUser: admin.auth.UserRecord | null = null
    try {
      try {
        firebaseUser = await auth.getUserByEmail(oktaUserData.primaryEmail)
        console.log(
          `ℹ️ User with email ${userData.email} already exists in Firebase (UID: ${firebaseUser.uid})`
        )
        const oktaProvider = firebaseUser.providerData.find(
          provider => provider.providerId === 'oidc.okta'
        )
        if (!oktaProvider && oktaUserData?.isSecondaryAccount === false) {
          firebaseUser = await auth.updateUser(firebaseUser.uid, {
            providerToLink: {
              providerId: 'oidc.okta',
              // use theKeySsoGuid from the OKTA response object because it is the correct one
              uid: oktaUserData?.theKeySsoGuid,
              displayName:
                `${oktaUserData.firstName} ${oktaUserData.lastName}`.trim(),
              email: oktaUserData.primaryEmail,
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
              email: oktaUserData.primaryEmail,
              emailVerified:
                oktaUserData.primaryEmailObject?.status === 'VERIFIED'
                  ? true
                  : false,
              displayName:
                `${oktaUserData.firstName} ${oktaUserData.lastName}`.trim(),
              disabled: false,
            })
            firebaseUser = await auth.updateUser(firebaseUser.uid, {
              providerToLink: {
                providerId: 'oidc.okta',
                uid: oktaUserData.theKeySsoGuid,
                displayName:
                  `${oktaUserData.firstName} ${oktaUserData.lastName}`.trim(),
                email: oktaUserData.primaryEmail,
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
            await writeErrorToFile(
              sourceDir,
              'users',
              filePath,
              error,
              userData
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
      await writeErrorToFile(sourceDir, 'users', filePath, error, userData)
      return null
    }

    // Save to database using Prisma
    try {
      if (!firebaseUser.email) {
        const error = new Error(
          `Firebase user dooes not have email for:  ${userData.email}`
        )
        console.error(`❌ ${error.message}`)
        await writeErrorToFile(sourceDir, 'users', filePath, error, userData)
        return null
      }

      // Check if user already exists (use lowercase for consistent lookups)
      const existingUser = await prismaApiUsers.user.findFirst({
        where: {
          email: firebaseUser.email.toLowerCase(),
        },
      })

      let userSavedToCore: User

      if (existingUser) {
        console.log(
          `✅ User ${firebaseUser.email} already exists in core database`
        )
        userSavedToCore = existingUser
        return userSavedToCore
      } else {
        // User doesn't exist, create (use lowercase email)
        const user: Prisma.UserCreateInput = {
          id: uuidv4(),
          userId: firebaseUser.uid,
          firstName: firebaseUser.displayName?.split(' ')[0] ?? '',
          lastName: firebaseUser.displayName?.split(' ')[1] ?? '',
          email: firebaseUser.email.toLowerCase(),
          emailVerified: firebaseUser.emailVerified,
          superAdmin: false,
        }

        userSavedToCore = await prismaApiUsers.user.create({
          data: user,
        })
        console.log(`✅ Created user ${firebaseUser.email} in core database`)
      }

      const userToSaveToLocal = {
        ownerId: userData.owner,
        email: firebaseUser.email.toLowerCase(),
        ssoGuid: oktaUserData.theKeySsoGuid,
        coreId: userSavedToCore.id,
        isSecondaryAccount: oktaUserData?.isSecondaryAccount ?? true,
      }

      await prismaUsers.user.create({
        data: userToSaveToLocal,
      })
      console.log(`✅ Saved user ${firebaseUser.email} to local database`)

      return userSavedToCore
    } catch (dbError) {
      console.error(`❌ Database error for user ${userData.owner}:`, dbError)
      await writeErrorToFile(sourceDir, 'users', filePath, dbError, userData)
      return null
    }
  } catch (error) {
    console.error(`❌ Error processing user file ${filePath}:`, error)
    // Try to read rawData if available, otherwise use undefined
    let rawData: unknown
    try {
      const fileContent = await fs.readFile(filePath, 'utf8')
      rawData = JSON.parse(fileContent)
    } catch {
      rawData = undefined
    }
    await writeErrorToFile(sourceDir, 'users', filePath, error, rawData)
    return null
  }
}

/**
 * Get all user JSON files from both user directories
 * @param sourceDir Base source directory
 * @param fileName Optional specific file name to filter by
 * @returns Array of file paths
 */
async function getUserFiles(
  sourceDir: string,
  fileName?: string
): Promise<string[]> {
  const userDirs = ['user', 'u']
  const allFiles: string[] = []

  // Normalize fileName - ensure it has .json extension if provided
  const normalizedFileName = fileName
    ? fileName.endsWith('.json')
      ? fileName
      : `${fileName}.json`
    : undefined

  for (const userDir of userDirs) {
    const fullPath = path.join(sourceDir, userDir)
    try {
      const files = await fs.readdir(fullPath)
      const jsonFiles = files
        .filter(file => {
          if (!file.endsWith('.json')) return false
          if (normalizedFileName) {
            return file === normalizedFileName
          }
          return true
        })
        .map(file => path.join(fullPath, file))
      allFiles.push(...jsonFiles)
    } catch (error) {
      console.warn(`⚠️ Could not read directory ${fullPath}:`, error)
      // Continue with other directories even if one fails
    }
  }

  return allFiles
}

export interface UserIngestionSummary {
  successCount: number
  errorCount: number
  totalFiles: number
  processedUsers: (User | UserLocal)[]
}

/**
 * Ingest users from cache directory
 * @param options Options for user ingestion
 * @returns Summary of user ingestion
 */
export async function ingestUsers(
  options: { sourceDir?: string; dryRun?: boolean; file?: string } = {}
): Promise<UserIngestionSummary | null> {
  const { sourceDir = './tmp', dryRun = false, file } = options

  console.log('👥 Starting user ingestion pipeline...')
  console.log(`📁 Source directory: ${sourceDir}`)
  console.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)
  if (file) {
    console.log(`📄 Processing single file: ${file}`)
  }

  // Clear errors directory at the beginning
  await clearErrorsDirectory(sourceDir, 'users')

  // Get all user files from both user and u directories
  const userFiles = await getUserFiles(sourceDir, file)
  if (userFiles.length === 0) {
    if (file) {
      console.log(`ℹ️ File ${file} not found in user/ or u/ directories`)
    } else {
      console.log('ℹ️ No user files found in user/ or u/ directories')
    }
    return null
  }

  console.log(`📊 Found ${userFiles.length} user files to process`)

  // Process each user file
  const processedUsers: (User | UserLocal)[] = []
  let successCount = 0
  let errorCount = 0

  for (const filePath of userFiles) {
    const processedUser = await processUserFile(filePath, sourceDir, dryRun)
    if (processedUser) {
      processedUsers.push(processedUser)
      successCount++
    } else {
      errorCount++
    }
  }

  if (dryRun) {
    console.log('\n🔍 Dry run - showing sample processed users:')
    processedUsers.slice(0, 3).forEach((user, index) => {
      console.log(`\nUser ${index + 1}:`)
      if ('firstName' in user && 'lastName' in user) {
        console.log(`  Name: ${user.firstName} ${user.lastName ?? ''}`)
      }
      console.log(`  Email: ${user.email}`)
      if ('userId' in user) {
        console.log(`  User ID: ${user.userId}`)
      }
    })
  }

  return {
    successCount,
    errorCount,
    totalFiles: userFiles.length,
    processedUsers,
  }
}
