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
import {
  writeErrorToFile,
  clearErrorsDirectory,
} from '../../lib/error-handler.js'
import cliProgress from 'cli-progress'
import { Logger } from '../../lib/logger.js'

// Zod schemas for playlist data validation
const PlaylistItemSchema = z.object({
  createdAt: z.string(),
  languageId: z.number(),
  mediaComponentId: z.string(),
  type: z.string().nullish(),
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
  channels: z.record(z.string(), z.union([z.null(), z.object({})])).nullish(),
  access: z.record(z.string(), z.record(z.string(), z.number())).nullish(),
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
  type?: string | null
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
  type: 'playlist'
}

type DeletedPlaylist = {
  type: 'deleted'
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
async function validateAndTransformPlaylist(
  rawData: unknown,
  sourceDir: string,
  fileID: string,
  logger: Logger
): Promise<ProcessedPlaylist | null> {
  try {
    // Parse and validate the raw data with Zod
    const parseResult = PlaylistDocumentSchema.safeParse(rawData)

    if (!parseResult.success) {
      logger.warn(
        '⚠️ Playlist document validation failed:',
        parseResult.error.issues
      )
      await writeErrorToFile(
        sourceDir,
        'playlists',
        fileID,
        parseResult.error,
        logger,
        rawData
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
      type: 'playlist',
    }
  } catch (error) {
    logger.error('❌ Error validating playlist data:', error)
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
  sourceDir: string,
  dryRun: boolean,
  logger: Logger
): Promise<ProcessedPlaylist | DeletedPlaylist | null> {
  try {
    const fileContent = await fs.readFile(filePath, 'utf8')
    const rawData = JSON.parse(fileContent)
    if (rawData?.['JFM-profiles']?._deleted === true) {
      return {
        type: 'deleted',
      }
    }
    const fileID = path.basename(filePath, '.json')

    const processedPlaylist = await validateAndTransformPlaylist(
      rawData,
      sourceDir,
      fileID,
      logger
    )
    if (!processedPlaylist) {
      logger.log(
        `⏭️ Skipping invalid playlist file: ${path.basename(filePath)}`
      )
      await writeErrorToFile(
        sourceDir,
        'playlists',
        filePath,
        new Error('Invalid playlist file'),
        logger,
        rawData
      )
      return null
    }
    if (dryRun) {
      logger.log(`⏭️ Skipping playlist ${processedPlaylist.name} in dry run`)
      return processedPlaylist
    }
    try {
      const relatedUser = await prismaUsers.user.findUnique({
        where: {
          ownerId: processedPlaylist.owner,
        },
      })
      if (!relatedUser) {
        const error = new Error(
          `User not found for playlist ${processedPlaylist.name}`
        )
        logger.error(`❌ ${error.message}`)
        await writeErrorToFile(
          sourceDir,
          'playlists',
          filePath,
          error,
          logger,
          processedPlaylist
        )
        throw error
      }
      // Check if playlist already exists to determine if we need to generate a slug
      const existingPlaylist = await prismaApiMedia.playlist.findUnique({
        where: { id: processedPlaylist.id },
      })

      const slug = existingPlaylist?.slug || (await generateUniqueSlug())

      const playListToCreate: PrismaApiMedia.PlaylistCreateInput = {
        id: processedPlaylist.id,
        name: processedPlaylist.name,
        note: processedPlaylist.note,
        noteUpdatedAt: processedPlaylist.noteModifiedAt,
        ownerId: relatedUser.coreId,
        createdAt: processedPlaylist.createdAt,
        updatedAt: processedPlaylist.updatedAt,
        slug,
      }

      const playListToUpdate: PrismaApiMedia.PlaylistUpdateInput = {
        name: processedPlaylist.name,
        note: processedPlaylist.note,
        noteUpdatedAt: processedPlaylist.noteModifiedAt,
        ownerId: relatedUser.coreId,
        updatedAt: processedPlaylist.updatedAt,
        // Don't update slug or createdAt on existing playlists
      }

      await prismaApiMedia.playlist.upsert({
        where: { id: processedPlaylist.id },
        update: playListToUpdate,
        create: playListToCreate,
      })

      // Save playlist items
      if (processedPlaylist.items.length > 0) {
        const savedItems: ProcessedPlaylistItem[] = []
        const skippedItems: ProcessedPlaylistItem[] = []

        for (const item of processedPlaylist.items) {
          try {
            const videoVariant = await prismaApiMedia.videoVariant.findUnique({
              where: {
                languageId_videoId: {
                  languageId: item.languageId.toString(),
                  videoId: item.mediaComponentId,
                },
              },
            })
            if (!videoVariant) {
              logger.warn(
                `⚠️ VideoVariant not found for mediaComponentId: ${item.mediaComponentId} (playlist: ${processedPlaylist.name})`
              )
              const error = new Error(
                `VideoVariant not found for mediaComponentId: ${item.mediaComponentId}`
              )
              const errorFilePath = `${processedPlaylist.id}-${item.order}-${item.mediaComponentId}.json`
              await writeErrorToFile(
                sourceDir,
                'playListItems',
                errorFilePath,
                error,
                logger,
                {
                  playlistId: processedPlaylist.id,
                  playlistName: processedPlaylist.name,
                  item,
                }
              )
              skippedItems.push(item)
              continue
            }

            // Check if playlist item already exists (by playlistId and order)
            const existingItem = await prismaApiMedia.playlistItem.findUnique({
              where: {
                playlistId_order: {
                  playlistId: processedPlaylist.id,
                  order: item.order,
                },
              },
            })

            // Use existing ID if found, otherwise generate new one
            const itemId = existingItem?.id || uuidv4()

            // Upsert playlist item
            const playlistItemToSave: PrismaApiMedia.PlaylistItemCreateInput = {
              id: itemId,
              order: item.order,
              createdAt: item.createdAt,
              updatedAt: item.updatedAt,
              Playlist: {
                connect: { id: processedPlaylist.id },
              },
              VideoVariant: {
                connect: {
                  languageId_videoId: {
                    languageId: videoVariant.languageId,
                    videoId: videoVariant.videoId,
                  },
                },
              },
            }

            await prismaApiMedia.playlistItem.upsert({
              where: { id: itemId },
              update: {
                order: item.order,
                updatedAt: item.updatedAt,
                VideoVariant: {
                  connect: { id: videoVariant.id },
                },
              },
              create: playlistItemToSave,
            })
            savedItems.push(item)
          } catch (itemError) {
            logger.error(
              `❌ Error saving playlist item for mediaComponentId ${item.mediaComponentId}:`,
              itemError
            )
            const errorFilePath = `${processedPlaylist.id}-${item.order}-${item.mediaComponentId}.json`
            await writeErrorToFile(
              sourceDir,
              'playListItems',
              errorFilePath,
              itemError,
              logger,
              {
                playlistId: processedPlaylist.id,
                playlistName: processedPlaylist.name,
                item,
              }
            )
            skippedItems.push(item)
          }
        }

        // Update processedPlaylist with saved and skipped items
        processedPlaylist.savedItems = savedItems
        processedPlaylist.skippedItems = skippedItems

        logger.log(
          `  📝 Saved ${savedItems.length} playlist items, skipped ${skippedItems.length}`
        )
      }
    } catch (error) {
      logger.error(`❌ Error saving playlist to local database:`, error)
      await writeErrorToFile(
        sourceDir,
        'playlists',
        filePath,
        error,
        logger,
        processedPlaylist
      )
      return null
    }

    logger.log(
      `✅ Processed playlist: ${processedPlaylist.name} (${processedPlaylist.itemCount} items)`
    )
    return processedPlaylist
  } catch (error) {
    logger.error(`❌ Error processing playlist file ${filePath}:`, error)
    // Try to read rawData if available, otherwise use undefined
    let rawData: unknown
    try {
      const fileContent = await fs.readFile(filePath, 'utf8')
      rawData = JSON.parse(fileContent)
    } catch {
      rawData = undefined
    }
    await writeErrorToFile(
      sourceDir,
      'playlists',
      filePath,
      error,
      logger,
      rawData
    )
    return null
  }
}

/**
 * Get all playlist JSON files from the playlist directory
 * @param playlistDir Path to the playlist directory
 * @param fileName Optional specific file name to filter by
 * @returns Array of file paths
 */
async function getPlaylistFiles(
  playlistDir: string,
  logger: Logger,
  fileName?: string
): Promise<string[]> {
  try {
    // Normalize fileName - ensure it has .json extension if provided
    const normalizedFileName = fileName
      ? fileName.endsWith('.json')
        ? fileName
        : `${fileName}.json`
      : undefined

    const files = await fs.readdir(playlistDir)
    return files
      .filter(file => {
        if (!file.endsWith('.json')) return false
        if (normalizedFileName) {
          return file === normalizedFileName
        }
        return true
      })
      .map(file => path.join(playlistDir, file))
  } catch (error) {
    logger.error(`❌ Error reading playlist directory ${playlistDir}:`, error)
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
  totalItemsNotProcessed: number
  videoVariantsNotFound: number
  uniqueMediaComponents: Set<string>
  languageDistribution: Map<number, number>
  averageItemsPerPlaylist: number
} {
  const uniqueMediaComponents = new Set<string>()
  const languageDistribution = new Map<number, number>()
  const uniqueSkippedVideoVariants = new Set<string>()
  let totalItems = 0
  let totalSavedItems = 0
  let totalSkippedItems = 0

  for (const playlist of playlists) {
    totalItems += playlist.itemCount
    totalSavedItems += playlist.savedItems.length
    totalSkippedItems += playlist.skippedItems.length

    for (const item of playlist.skippedItems) {
      uniqueSkippedVideoVariants.add(
        `${item.mediaComponentId}-${item.languageId}`
      )
    }

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
    totalItemsNotProcessed: 0,
    videoVariantsNotFound: uniqueSkippedVideoVariants.size,
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
  /** Count of unique VideoVariants that were not found when saving playlist items */
  videoVariantsNotFound: number
}

/**
 * Ingest playlists from cache directory
 * @param options Options for playlist ingestion
 * @returns Summary of playlist ingestion
 */
export async function ingestPlaylists(
  options: {
    sourceDir?: string
    dryRun?: boolean
    file?: string
    concurrency?: number
    debug?: boolean
  } = {}
): Promise<PlaylistIngestionSummary | null> {
  const {
    sourceDir = './tmp',
    dryRun = false,
    file,
    concurrency = 10,
    debug = false,
  } = options
  const logger = new Logger(debug)
  const playlistDir = path.join(sourceDir, 'pl')

  logger.log('🎵 Starting playlist ingestion pipeline...')
  logger.log(`📁 Source directory: ${playlistDir}`)
  logger.log(`🔍 Dry run: ${dryRun ? 'Yes' : 'No'}`)
  logger.log(`⚡ Concurrency: ${concurrency}`)
  if (file) {
    logger.log(`📄 Processing single file: ${file}`)
  }

  // Clear errors directory at the beginning
  await clearErrorsDirectory(sourceDir, 'playlists', logger)
  await clearErrorsDirectory(sourceDir, 'playListItems', logger)

  // Check if playlist directory exists
  try {
    await fs.access(playlistDir)
  } catch {
    logger.error(`❌ Playlist directory does not exist: ${playlistDir}`)
    return null
  }

  // Get all playlist files
  const playlistFiles = await getPlaylistFiles(playlistDir, logger, file)
  if (playlistFiles.length === 0) {
    if (file) {
      logger.info(`ℹ️ File ${file} not found in playlist directory`)
    } else {
      logger.info('ℹ️ No playlist files found in directory')
    }
    return null
  }

  logger.log(`📊 Found ${playlistFiles.length} playlist files to process`)

  let progressBar: cliProgress.SingleBar | null = null
  if (!debug) {
    progressBar = new cliProgress.SingleBar(
      {
        format:
          '🎵 Ingesting playlists |{bar}| {percentage}% | {value}/{total} files | ETA: {eta}s',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic
    )
    progressBar.start(playlistFiles.length, 0)
  }

  // Process playlist files with concurrency limit
  const processedPlaylists: ProcessedPlaylist[] = []
  let successCount = 0
  let errorCount = 0

  for (let i = 0; i < playlistFiles.length; i += concurrency) {
    const batch = playlistFiles.slice(i, i + concurrency)

    const results = await Promise.allSettled(
      batch.map(filePath =>
        processPlaylistFile(filePath, sourceDir, dryRun, logger)
      )
    )

    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        if (result.value.type === 'playlist') {
          processedPlaylists.push(result.value)
        }
        successCount++
      } else {
        errorCount++
      }
      if (progressBar) {
        progressBar.update(successCount + errorCount)
      }
    }
  }

  if (progressBar) {
    progressBar.stop()
  }

  // Analyze playlist data
  const analysis = analyzePlaylistItems(processedPlaylists)

  if (dryRun) {
    logger.info('\n🔍 Dry run - showing sample processed playlists:')
    processedPlaylists.slice(0, 3).forEach((playlist, index) => {
      logger.info(`\nPlaylist ${index + 1}:`)
      logger.info(`  Name: ${playlist.name}`)
      logger.info(`  Display Name: ${playlist.displayName}`)
      logger.info(`  Owner: ${playlist.owner}`)
      logger.info(`  Items: ${playlist.itemCount}`)
      logger.info(`  Created: ${playlist.createdAt.toISOString()}`)
    })
  } else {
    // TODO: Implement actual ingestion to Core system
    logger.info('\n🚀 Ready to ingest playlists to Core system')
    logger.info(`📊 ${processedPlaylists.length} playlists ready for ingestion`)
  }

  return {
    successCount,
    errorCount,
    totalFiles: playlistFiles.length,
    analysis,
    processedPlaylists,
    videoVariantsNotFound: analysis.videoVariantsNotFound,
  }
}
