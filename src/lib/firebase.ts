import admin, { type ServiceAccount } from 'firebase-admin'
import { env } from './env.js'

export const firebaseClient = admin.initializeApp(
  env.GOOGLE_APPLICATION_JSON != null && env.GOOGLE_APPLICATION_JSON !== ''
    ? {
        credential: admin.credential.cert(
          JSON.parse(env.GOOGLE_APPLICATION_JSON) as ServiceAccount
        ),
      }
    : undefined
)

export const auth = firebaseClient.auth()
