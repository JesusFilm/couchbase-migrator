import { z } from 'zod'

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

export const UserProfileSchema = z.object({
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

export type UserProfile = z.infer<typeof UserProfileSchema> & { cas: number }

// Okta API user response type
export interface OktaUser {
  id: string
  status:
    | 'ACTIVE'
    | 'DEPROVISIONED'
    | 'LOCKED_OUT'
    | 'PASSWORD_EXPIRED'
    | 'PROVISIONED'
    | 'RECOVERY'
    | 'STAGED'
    | 'SUSPENDED'
  activated: string | null // ISO date-time
  created: string // ISO date-time
  lastLogin: string | null // ISO date-time
  lastUpdated: string // ISO date-time
  passwordChanged: string | null // ISO date-time
  statusChanged: string | null // ISO date-time
  transitioningToStatus: 'ACTIVE' | 'DEPROVISIONED' | 'PROVISIONED' | null
  realmId: string
  profile: {
    firstName?: string
    lastName?: string
    theKeyGuid: string
  }
  credentials?: {
    emails?: {
      type: 'PRIMARY' | 'SECONDARY'
      status: 'VERIFIED' | 'UNVERIFIED'
      value: string
    }[]
    password?: {
      value?: string
      hash?: {
        algorithm: string
        salt?: string
        saltOrder?: string
        value: string
        workFactor?: number
      }
      hook?: {
        type: string
      }
    }
    provider?: {
      name: string
      type:
        | 'ACTIVE_DIRECTORY'
        | 'FEDERATION'
        | 'IMPORT'
        | 'LDAP'
        | 'OKTA'
        | 'SOCIAL'
    }
    recovery_question?: {
      question: string
    }
  }
}
