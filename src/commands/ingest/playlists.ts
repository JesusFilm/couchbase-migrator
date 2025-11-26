/**
 * Playlist Ingestion Pipeline
 *
 * Handles ingesting playlist documents from the cache into Core
 */

import { promises as fs } from 'fs'
import path from 'path'
import { z } from 'zod'
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
  createdAt: z.string().optional(),
  note: z.string().optional().default(''),
  noteModifiedAt: z.string().optional(),
  owner: z.string(),
  playlistByDisplayName: z.string().optional(),
  playlistItems: z.array(PlaylistItemSchema).optional().default([]),
  playlistName: z.string(),
  type: z.literal('playlist'),
  updatedAt: z.string().optional(),
})

const PlaylistDocumentSchema = z.object({
  'JFM-profiles': PlaylistProfileSchema,
  cas: z.number(),
})

// Inferred types from Zod schemas
type PlaylistItem = z.infer<typeof PlaylistItemSchema>
type ProcessedPlaylist = {
  id: string
  name: string
  displayName: string
  note: string
  noteModifiedAt: Date
  owner: string
  items: PlaylistItem[]
  itemCount: number
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
  rawData: unknown
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

    return {
      id: playlistData.owner, // Use owner as the playlist ID
      name: playlistData.playlistName,
      displayName:
        playlistData.playlistByDisplayName || playlistData.playlistName,
      note: playlistData.note || '',
      noteModifiedAt: new Date(
        playlistData.noteModifiedAt ||
          playlistData.createdAt ||
          new Date().toISOString()
      ),
      owner: playlistData.owner,
      items: playlistData.playlistItems || [],
      itemCount: (playlistData.playlistItems || []).length,
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

    const processedPlaylist = validateAndTransformPlaylist(rawData)
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
  uniqueMediaComponents: Set<string>
  languageDistribution: Map<number, number>
  averageItemsPerPlaylist: number
} {
  const uniqueMediaComponents = new Set<string>()
  const languageDistribution = new Map<number, number>()
  let totalItems = 0

  for (const playlist of playlists) {
    totalItems += playlist.itemCount

    for (const item of playlist.items) {
      uniqueMediaComponents.add(item.mediaComponentId)

      const currentCount = languageDistribution.get(item.languageId) || 0
      languageDistribution.set(item.languageId, currentCount + 1)
    }
  }

  return {
    totalItems,
    uniqueMediaComponents,
    languageDistribution,
    averageItemsPerPlaylist:
      playlists.length > 0 ? totalItems / playlists.length : 0,
  }
}

/**
 * Ingest playlists from cache directory
 * @param options Options for playlist ingestion
 */
export async function ingestPlaylists(
  options: { sourceDir?: string; dryRun?: boolean } = {}
): Promise<void> {
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
    return
  }

  // Get all playlist files
  const playlistFiles = await getPlaylistFiles(playlistDir)
  if (playlistFiles.length === 0) {
    console.log('ℹ️ No playlist files found in directory')
    return
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

  // Summary
  console.log('\n📈 Playlist Ingestion Summary:')
  console.log(`✅ Successfully processed: ${successCount} playlists`)
  console.log(`❌ Failed to process: ${errorCount} playlists`)
  console.log(`📊 Total files: ${playlistFiles.length}`)
  console.log(`🎵 Total playlist items: ${analysis.totalItems}`)
  console.log(
    `📺 Unique media components: ${analysis.uniqueMediaComponents.size}`
  )
  console.log(
    `📊 Average items per playlist: ${analysis.averageItemsPerPlaylist.toFixed(2)}`
  )

  // Language distribution
  if (analysis.languageDistribution.size > 0) {
    console.log('\n🌍 Language Distribution:')
    const sortedLanguages = Array.from(analysis.languageDistribution.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5) // Show top 5 languages

    for (const [languageId, count] of sortedLanguages) {
      console.log(`  Language ${languageId}: ${count} items`)
    }
  }

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
}
