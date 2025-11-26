import { createEnv } from '@t3-oss/env-core'
import { z } from 'zod'
import { config } from 'dotenv'

// Load environment variables from .env file
config()

export const env = createEnv({
  server: {
    // Couchbase Server Configuration
    COUCHBASE_CONNECTION_STRING: z.string().default('couchbase://localhost'),
    COUCHBASE_USERNAME: z.string().default('Administrator'),
    COUCHBASE_PASSWORD: z.string().default('password'),

    // Bucket Configuration
    COUCHBASE_BUCKET_NAME: z.string().default('default'),

    // Connection Options
    COUCHBASE_CONNECTION_TIMEOUT: z.coerce.number().default(10000),
    COUCHBASE_OPERATION_TIMEOUT: z.coerce.number().default(5000),

    // SSL Configuration
    COUCHBASE_TRUST_STORE_PATH: z.string().optional(),

    // Prisma Database URLs
    PRISMA_API_USERS_URL: z.string(),
    PRISMA_API_MEDIA_URL: z.string(),
    PRISMA_USERS_URL: z.string().optional(),
  },
  runtimeEnv: {
    COUCHBASE_CONNECTION_STRING: process.env['COUCHBASE_CONNECTION_STRING'],
    COUCHBASE_USERNAME: process.env['COUCHBASE_USERNAME'],
    COUCHBASE_PASSWORD: process.env['COUCHBASE_PASSWORD'],
    COUCHBASE_BUCKET_NAME: process.env['COUCHBASE_BUCKET_NAME'],
    COUCHBASE_CONNECTION_TIMEOUT: process.env['COUCHBASE_CONNECTION_TIMEOUT'],
    COUCHBASE_OPERATION_TIMEOUT: process.env['COUCHBASE_OPERATION_TIMEOUT'],
    COUCHBASE_TRUST_STORE_PATH: process.env['COUCHBASE_TRUST_STORE_PATH'],
    PRISMA_API_USERS_URL: process.env['PRISMA_API_USERS_URL'],
    PRISMA_API_MEDIA_URL: process.env['PRISMA_API_MEDIA_URL'],
    PRISMA_USERS_URL: process.env['PRISMA_USERS_URL'],
  },
  skipValidation: !!process.env['SKIP_ENV_VALIDATION'],
})
