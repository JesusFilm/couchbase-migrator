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
 * Retry Okta API call with exponential backoff for rate limits
 * @param fn Function that returns a fetch Response
 * @param maxRetries Maximum number of retries (default: 5)
 * @param baseDelay Base delay in milliseconds (default: 1000)
 * @returns Promise that resolves with the Response
 */
async function fetchOktaWithRetry(
  fn: () => Promise<Response>,
  maxRetries: number = 5,
  baseDelay: number = 5000
): Promise<Response> {
  let lastResponse: Response | undefined

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const response = await fn()

    // Success - return immediately
    if (response.ok) {
      return response
    }

    // 429 rate limit - retry with backoff
    if (response.status === 429) {
      lastResponse = response

      if (attempt === maxRetries) {
        console.error(
          `❌ Okta rate limit hit after ${maxRetries} attempts. Giving up.`
        )
        return response
      }

      // Check for x-rate-limit-reset header (Unix timestamp)
      const rateLimitReset = response.headers.get('x-rate-limit-reset')
      let delay: number

      if (rateLimitReset) {
        // Use x-rate-limit-reset header (Unix timestamp in seconds)
        const resetTime = parseInt(rateLimitReset, 10) * 1000
        const now = Date.now()
        const bufferMs = 500 // 500ms buffer to ensure rate limit has reset
        delay = Math.max(0, resetTime - now + bufferMs)
        console.warn(
          `⚠️ Okta rate limit (429) - x-rate-limit-reset: ${rateLimitReset} (${new Date(resetTime).toISOString()}). Waiting ${delay}ms before retry ${attempt + 1}/${maxRetries}`
        )
      } else {
        // Exponential backoff: baseDelay * 2^(attempt-1)
        delay = baseDelay * Math.pow(2, attempt - 1)
        console.warn(
          `⚠️ Okta rate limit (429) - No x-rate-limit-reset header. Retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`
        )
      }

      await new Promise(resolve => setTimeout(resolve, delay))
      continue
    }

    // Other errors - return immediately (don't retry)
    return response
  }

  return lastResponse!
}

/**
 * Process a single user JSON file
 * @param filePath Path to the user JSON file
 * @param sourceDir Base source directory for error files
 * @param dryRun Whether this is a dry run
 * @param oktaToken Okta token to use for API calls
 * @returns Processed user data or null if processing failed
 */
async function processUserFile(
  filePath: string,
  sourceDir: string,
  dryRun: boolean,
  oktaToken: string
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

    // Check if user already exists in local database
    // check by SSO GUID since it is unique
    const existingLocalUser = await prismaUsers.user.findUnique({
      where: {
        ssoGuid: userData.theKeySsoGuid,
      },
    })
    if (existingLocalUser) {
      console.log(
        `✅ User with ssoGuid ${userData.theKeySsoGuid} and email ${userData.email} already exists in local database`
      )
      return existingLocalUser
    }

    // Fetch user from Okta API by SSO GUID
    let oktaUserData

    try {
      const filterExpression = `profile.theKeyGuid eq "${userData.theKeySsoGuid.trim()}"`
      const token = oktaToken
      const oktaResponse = await fetchOktaWithRetry(
        () =>
          fetch(
            `https://signon.okta.com/api/v1/users?search=${encodeURIComponent(filterExpression)}`,
            {
              headers: {
                Authorization: `SSWS ${token}`,
                Accept: 'application/json',
              },
            }
          ),
        5, // max retries
        5000 // base delay 5 second
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
          return null
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

        if (responseData.length > 1) {
          console.warn(
            `⚠️ Multiple users found in Okta for SSO GUID ${userData.theKeySsoGuid}`
          )
          await writeErrorToFile(
            sourceDir,
            'users',
            filePath,
            new Error('Multiple users found in Okta response'),
            userData
          )
          return null
        }

        // Get the first user from the array (should only be one for exact SSO match)
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
          firstName: resData.profile.firstName,
          lastName: resData.profile.lastName,
          status: resData.status,
          primaryEmail: primaryEmail.value,
          primaryEmailObject: primaryEmail,
          theKeySsoGuid: resData.profile.theKeyGuid,
        }

        console.log(
          `✅ Fetched Okta user data for email ${userData.email} and ssoGuid ${userData.theKeySsoGuid}:`,
          {
            id: oktaUserData?.id,
            email: userData?.email,
            status: oktaUserData?.status,
            firstName: oktaUserData?.firstName,
            lastName: oktaUserData?.lastName,
            primaryEmail: oktaUserData?.primaryEmail,
            emails: emails?.map(email => email.value),
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
        if (!oktaProvider) {
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
  options: {
    sourceDir?: string
    dryRun?: boolean
    file?: string
    concurrency?: number
  } = {}
): Promise<UserIngestionSummary | null> {
  const {
    sourceDir = './tmp',
    dryRun = false,
    file,
    concurrency = 10,
  } = options

  console.log('👥 Starting user ingestion pipeline...')
  console.log(`📁 Source directory: ${sourceDir}`)
  console.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)
  console.log(`⚡ Concurrency: ${concurrency} (balanced across 2 Okta tokens)`)
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

  // Process user files with concurrency limit, balancing between two tokens
  const processedUsers: (User | UserLocal)[] = []
  let successCount = 0
  let errorCount = 0

  for (let i = 0; i < userFiles.length; i += concurrency) {
    const batch = userFiles.slice(i, i + concurrency)
    const halfBatch = Math.ceil(batch.length / 2)

    // Split batch: first half uses OKTA_TOKEN, second half uses OKTA_TOKEN_2
    const firstHalf = batch.slice(0, halfBatch)
    const secondHalf = batch.slice(halfBatch)

    const results = await Promise.allSettled([
      ...firstHalf.map(filePath =>
        processUserFile(filePath, sourceDir, dryRun, env.OKTA_TOKEN)
      ),
      ...secondHalf.map(filePath =>
        processUserFile(filePath, sourceDir, dryRun, env.OKTA_TOKEN_2)
      ),
    ])

    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        processedUsers.push(result.value)
        successCount++
      } else {
        errorCount++
      }
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
