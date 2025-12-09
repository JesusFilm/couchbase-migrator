/**
 * Playlist Ingestion Pipeline
 *
 * Handles ingesting playlist documents from the cache into Core
 */

import { promises as fs } from 'fs'
import path from 'path'
import { z } from 'zod'
import { v4 as uuidv4 } from 'uuid'
import { prismaUsers } from '../../lib/prisma/users/client'
import {
  prismaApiMedia,
  Prisma as PrismaApiMedia,
} from '../../lib/prisma/api-media/client'

// Zod schemas for playlist data validation
const PlaylistItemSchema = z.object({
  createdAt: z.string(),
  languageId: z.number(),
  mediaComponentId: z.string(),
  type: z.string().optional(),
})

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

const PlaylistProfileSchema = z.object({
  _sync: SyncDataSchema,
  createdAt: z.string().nullish(),
  note: z.string().nullish().default(''),
  noteModifiedAt: z.string().nullish(),
  owner: z.string(),
  playlistByDisplayName: z.string().nullish(),
  playlistItems: z.array(PlaylistItemSchema).nullish().default([]),
  playlistName: z.string().nullish(),
  type: z.literal('playlist'),
  updatedAt: z.string().nullish(),
})

const PlaylistDocumentSchema = z.object({
  'JFM-profiles': PlaylistProfileSchema,
  cas: z.number(),
})

// Inferred types from Zod schemas
type ProcessedPlaylistItem = {
  order: number
  createdAt: Date
  updatedAt: Date
  mediaComponentId: string // Used to look up VideoVariant by slug
  languageId: number
  type?: string | undefined
}
type ProcessedPlaylist = {
  id: string
  name: string
  displayName: string
  note: string
  noteModifiedAt: Date
  owner: string
  items: ProcessedPlaylistItem[]
  itemCount: number
  savedItems: ProcessedPlaylistItem[]
  skippedItems: ProcessedPlaylistItem[]
  createdAt: Date
  updatedAt: Date
  cas: number
}

async function generateUniqueSlug(): Promise<string> {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
  const maxAttempts = 10

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    let result = ''
    for (let i = 0; i < 6; i++) {
      result += chars.charAt(Math.floor(Math.random() * chars.length))
    }

    const existing = await prismaApiMedia.playlist.findUnique({
      where: { slug: result },
    })

    if (!existing) {
      return result
    }
  }

  throw new Error('Unable to generate unique slug after 10 attempts')
}

/**
 * Validate and transform playlist document data using Zod
 * @param rawData Raw JSON data from file
 * @returns Processed playlist data or null if invalid
 */
function validateAndTransformPlaylist(
  rawData: unknown,
  fileID: string
): ProcessedPlaylist | null {
  try {
    // Parse and validate the raw data with Zod
    const parseResult = PlaylistDocumentSchema.safeParse(rawData)

    if (!parseResult.success) {
      console.warn(
        '⚠️ Playlist document validation failed:',
        parseResult.error.issues
      )
      return null
    }

    const playlistData = parseResult.data['JFM-profiles']

    // Transform playlist items with order and proper date conversion
    const rawItems = playlistData.playlistItems || []
    const transformedItems: ProcessedPlaylistItem[] = rawItems.map(
      (item, index) => {
        const createdAt = new Date(item.createdAt || new Date().toISOString())
        const transformedItem: ProcessedPlaylistItem = {
          order: index,
          createdAt,
          updatedAt: createdAt, // Use createdAt as updatedAt if not available
          mediaComponentId: item.mediaComponentId,
          languageId: item.languageId,
        }
        if (item.type !== undefined) {
          transformedItem.type = item.type
        }
        return transformedItem
      }
    )

    return {
      id: fileID,
      name: playlistData.playlistName ?? '',
      displayName:
        playlistData.playlistByDisplayName ?? playlistData.playlistName ?? '',
      note: playlistData.note ?? '',
      noteModifiedAt: new Date(
        playlistData.noteModifiedAt ||
          playlistData.createdAt ||
          new Date().toISOString()
      ),
      owner: playlistData.owner,
      items: transformedItems,
      itemCount: transformedItems.length,
      savedItems: [], // Will be populated during processing
      skippedItems: [], // Will be populated during processing
      createdAt: new Date(playlistData.createdAt || new Date().toISOString()),
      updatedAt: new Date(playlistData.updatedAt || new Date().toISOString()),
      cas: parseResult.data.cas,
    }
  } catch (error) {
    console.error('❌ Error validating playlist data:', error)
    return null
  }
}

/**
 * Process a single playlist JSON file
 * @param filePath Path to the playlist JSON file
 * @returns Processed playlist data or null if processing failed
 */
async function processPlaylistFile(
  filePath: string,
  dryRun: boolean
): Promise<ProcessedPlaylist | null> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const rawData = JSON.parse(fileContent)
    const fileID = path.basename(filePath, '.json')

    const processedPlaylist = validateAndTransformPlaylist(rawData, fileID)
    if (!processedPlaylist) {
      console.log(
        `⏭️ Skipping invalid playlist file: ${path.basename(filePath)}`
      )
      return null
    }
    if (dryRun) {
      console.log(`⏭️ Skipping playlist ${processedPlaylist.name} in dry run`)
      return processedPlaylist
    }
    try {
      const relatedUser = await prismaUsers.user.findUnique({
        where: {
          ownerId: processedPlaylist.owner,
        },
      })
      if (!relatedUser) {
        console.error(
          `❌ User not found for playlist ${processedPlaylist.name}`
        )
        throw new Error(`User not found for playlist ${processedPlaylist.name}`)
      }
      const playListToSave: PrismaApiMedia.PlaylistCreateInput = {
        id: processedPlaylist.id,
        name: processedPlaylist.name,
        note: processedPlaylist.note,
        noteUpdatedAt: processedPlaylist.noteModifiedAt,
        ownerId: relatedUser.coreId,
        createdAt: processedPlaylist.createdAt,
        updatedAt: processedPlaylist.updatedAt,
        slug: await generateUniqueSlug(),
      }
      await prismaApiMedia.playlist.upsert({
        where: { id: processedPlaylist.id },
        update: playListToSave,
        create: playListToSave,
      })

      // Save playlist items
      if (processedPlaylist.items.length > 0) {
        const savedItems: ProcessedPlaylistItem[] = []
        const skippedItems: ProcessedPlaylistItem[] = []

        for (const item of processedPlaylist.items) {
          try {
            // Check if playlist item already exists (by playlistId and order)
            const existingItem = await prismaApiMedia.playlistItem.findFirst({
              where: {
                playlistId: processedPlaylist.id,
                order: item.order,
              },
            })

            if (existingItem) {
              skippedItems.push(item)
              continue
            }

            // Look up VideoVariant by slug (mediaComponentId)
            const videoVariant = await prismaApiMedia.videoVariant.findUnique({
              where: {
                languageId_videoId: {
                  languageId: item.languageId.toString(),
                  videoId: item.mediaComponentId,
                },
              },
            })
            if (!videoVariant) {
              console.warn(
                `⚠️ VideoVariant not found for mediaComponentId: ${item.mediaComponentId} (playlist: ${processedPlaylist.name})`
              )
              skippedItems.push(item)
              continue
            }

            // Create playlist item
            const playlistItemToSave: PrismaApiMedia.PlaylistItemCreateInput = {
              id: uuidv4(),
              order: item.order,
              createdAt: item.createdAt,
              updatedAt: item.updatedAt,
              Playlist: {
                connect: { id: processedPlaylist.id },
              },
              VideoVariant: {
                connect: { id: videoVariant.id },
              },
            }

            await prismaApiMedia.playlistItem.create({
              data: playlistItemToSave,
            })
            savedItems.push(item)
          } catch (itemError) {
            console.error(
              `❌ Error saving playlist item for mediaComponentId ${item.mediaComponentId}:`,
              itemError
            )
            skippedItems.push(item)
          }
        }

        // Update processedPlaylist with saved and skipped items
        processedPlaylist.savedItems = savedItems
        processedPlaylist.skippedItems = skippedItems

        console.log(
          `  📝 Saved ${savedItems.length} playlist items, skipped ${skippedItems.length}`
        )
      }
    } catch (error) {
      console.error(`❌ Error saving playlist to local database:`, error)
      return null
    }

    console.log(
      `✅ Processed playlist: ${processedPlaylist.name} (${processedPlaylist.itemCount} items)`
    )
    return processedPlaylist
  } catch (error) {
    console.error(`❌ Error processing playlist file ${filePath}:`, error)
    return null
  }
}

/**
 * Get all playlist JSON files from the playlist directory
 * @param playlistDir Path to the playlist directory
 * @returns Array of file paths
 */
async function getPlaylistFiles(playlistDir: string): Promise<string[]> {
  try {
    const files = await fs.readdir(playlistDir)
    return files
      .filter(file => file.endsWith('.json'))
      .map(file => path.join(playlistDir, file))
  } catch (error) {
    console.error(`❌ Error reading playlist directory ${playlistDir}:`, error)
    return []
  }
}

/**
 * Analyze playlist items to get statistics
 * @param playlists Array of processed playlists
 * @returns Statistics about playlist items
 */
function analyzePlaylistItems(playlists: ProcessedPlaylist[]): {
  totalItems: number
  totalSavedItems: number
  totalSkippedItems: number
  uniqueMediaComponents: Set<string>
  languageDistribution: Map<number, number>
  averageItemsPerPlaylist: number
} {
  const uniqueMediaComponents = new Set<string>()
  const languageDistribution = new Map<number, number>()
  let totalItems = 0
  let totalSavedItems = 0
  let totalSkippedItems = 0

  for (const playlist of playlists) {
    totalItems += playlist.itemCount
    totalSavedItems += playlist.savedItems.length
    totalSkippedItems += playlist.skippedItems.length

    for (const item of playlist.items) {
      uniqueMediaComponents.add(item.mediaComponentId)

      const currentCount = languageDistribution.get(item.languageId) || 0
      languageDistribution.set(item.languageId, currentCount + 1)
    }
  }

  return {
    totalItems,
    totalSavedItems,
    totalSkippedItems,
    uniqueMediaComponents,
    languageDistribution,
    averageItemsPerPlaylist:
      playlists.length > 0 ? totalItems / playlists.length : 0,
  }
}

export interface PlaylistIngestionSummary {
  successCount: number
  errorCount: number
  totalFiles: number
  analysis: ReturnType<typeof analyzePlaylistItems>
  processedPlaylists: ProcessedPlaylist[]
}

/**
 * Ingest playlists from cache directory
 * @param options Options for playlist ingestion
 * @returns Summary of playlist ingestion
 */
export async function ingestPlaylists(
  options: { sourceDir?: string; dryRun?: boolean } = {}
): Promise<PlaylistIngestionSummary | null> {
  const { sourceDir = './tmp', dryRun = false } = options
  const playlistDir = path.join(sourceDir, 'pl')

  console.log('🎵 Starting playlist ingestion pipeline...')
  console.log(`📁 Source directory: ${playlistDir}`)
  console.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)

  // Check if playlist directory exists
  try {
    await fs.access(playlistDir)
  } catch {
    console.error(`❌ Playlist directory does not exist: ${playlistDir}`)
    return null
  }

  // Get all playlist files
  const playlistFiles = await getPlaylistFiles(playlistDir)
  if (playlistFiles.length === 0) {
    console.log('ℹ️ No playlist files found in directory')
    return null
  }

  console.log(`📊 Found ${playlistFiles.length} playlist files to process`)

  // Process each playlist file
  const processedPlaylists: ProcessedPlaylist[] = []
  let successCount = 0
  let errorCount = 0

  for (const filePath of playlistFiles) {
    const processedPlaylist = await processPlaylistFile(filePath, dryRun)
    if (processedPlaylist) {
      processedPlaylists.push(processedPlaylist)
      successCount++
    } else {
      errorCount++
    }
  }

  // Analyze playlist data
  const analysis = analyzePlaylistItems(processedPlaylists)

  if (dryRun) {
    console.log('\n🔍 Dry run - showing sample processed playlists:')
    processedPlaylists.slice(0, 3).forEach((playlist, index) => {
      console.log(`\nPlaylist ${index + 1}:`)
      console.log(`  Name: ${playlist.name}`)
      console.log(`  Display Name: ${playlist.displayName}`)
      console.log(`  Owner: ${playlist.owner}`)
      console.log(`  Items: ${playlist.itemCount}`)
      console.log(`  Created: ${playlist.createdAt.toISOString()}`)
    })
  } else {
    // TODO: Implement actual ingestion to Core system
    console.log('\n🚀 Ready to ingest playlists to Core system')
    console.log(`📊 ${processedPlaylists.length} playlists ready for ingestion`)
  }

  return {
    successCount,
    errorCount,
    totalFiles: playlistFiles.length,
    analysis,
    processedPlaylists,
  }
}
