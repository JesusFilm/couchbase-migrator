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
    COUCHBASE_SCOPE_NAME: z.string().default('_default'),
    COUCHBASE_COLLECTION_NAME: z.string().default('_default'),

    // Connection Options
    COUCHBASE_CONNECTION_TIMEOUT: z.coerce.number().default(10000),
    COUCHBASE_OPERATION_TIMEOUT: z.coerce.number().default(5000),

    // SSL Configuration
    COUCHBASE_TRUST_STORE_PATH: z.string().optional(),
  },
  runtimeEnv: {
    COUCHBASE_CONNECTION_STRING: process.env['COUCHBASE_CONNECTION_STRING'],
    COUCHBASE_USERNAME: process.env['COUCHBASE_USERNAME'],
    COUCHBASE_PASSWORD: process.env['COUCHBASE_PASSWORD'],
    COUCHBASE_BUCKET_NAME: process.env['COUCHBASE_BUCKET_NAME'],
    COUCHBASE_SCOPE_NAME: process.env['COUCHBASE_SCOPE_NAME'],
    COUCHBASE_COLLECTION_NAME: process.env['COUCHBASE_COLLECTION_NAME'],
    COUCHBASE_CONNECTION_TIMEOUT: process.env['COUCHBASE_CONNECTION_TIMEOUT'],
    COUCHBASE_OPERATION_TIMEOUT: process.env['COUCHBASE_OPERATION_TIMEOUT'],
    COUCHBASE_TRUST_STORE_PATH: process.env['COUCHBASE_TRUST_STORE_PATH'],
  },
  skipValidation: !!process.env['SKIP_ENV_VALIDATION'],
})
