import { Cluster, Bucket, ConnectOptions } from 'couchbase'
import { env } from '@/lib/env'

export interface CouchbaseConfig {
  connectionString: string
  username: string
  password: string
  bucketName: string
  connectionTimeout: number
  operationTimeout: number
  trustStorePath?: string // Path to trust store file for SSL certificate validation
}

export class CouchbaseClient {
  private static instance: CouchbaseClient | null = null
  private cluster: Cluster | null = null
  private bucket: Bucket | null = null
  private config: CouchbaseConfig
  private isConnected: boolean = false
  private initPromise: Promise<CouchbaseClient> | null = null

  private constructor(config?: Partial<CouchbaseConfig>) {
    this.config = {
      connectionString: env.COUCHBASE_CONNECTION_STRING,
      username: env.COUCHBASE_USERNAME,
      password: env.COUCHBASE_PASSWORD,
      bucketName: env.COUCHBASE_BUCKET_NAME,
      connectionTimeout: env.COUCHBASE_CONNECTION_TIMEOUT,
      operationTimeout: env.COUCHBASE_OPERATION_TIMEOUT,
      ...(env.COUCHBASE_TRUST_STORE_PATH && {
        trustStorePath: env.COUCHBASE_TRUST_STORE_PATH,
      }),
      ...config,
    }
  }

  /**
   * Get the singleton instance of CouchbaseClient
   */
  public static getInstance(
    config?: Partial<CouchbaseConfig>
  ): CouchbaseClient {
    if (!CouchbaseClient.instance) {
      CouchbaseClient.instance = new CouchbaseClient(config)
    }
    return CouchbaseClient.instance
  }

  /**
   * Connect to Couchbase (auto-initializes if needed)
   */
  public async connect(): Promise<CouchbaseClient> {
    if (this.isConnected) {
      return this
    }

    if (this.initPromise) {
      return this.initPromise
    }

    this.initPromise = this._establishConnection().then(() => this)
    await this.initPromise
    return this
  }

  /**
   * Establish connection to Couchbase cluster (internal method)
   */
  private async _establishConnection(): Promise<void> {
    try {
      console.log('🔌 Connecting to Couchbase cluster...')
      console.log(`📍 Connection string: ${this.config.connectionString}`)
      console.log(`👤 Username: ${this.config.username}`)
      console.log(`🪣 Bucket: ${this.config.bucketName}`)

      const options: ConnectOptions = {
        username: this.config.username,
        password: this.config.password,
      }

      if (this.config.trustStorePath) {
        options.trustStorePath = this.config.trustStorePath
      }

      this.cluster = await Cluster.connect(
        this.config.connectionString,
        options
      )

      // Get bucket (using default scope and collection)
      this.bucket = this.cluster.bucket(this.config.bucketName)

      this.isConnected = true
      console.log('✅ Successfully connected to Couchbase!')
      console.log(`📊 Cluster info: ${await this.getClusterInfo()}`)
    } catch (error) {
      console.error('❌ Failed to connect to Couchbase:', error)
      throw error
    }
  }

  /**
   * Disconnect from Couchbase cluster
   */
  async disconnect(): Promise<void> {
    try {
      if (this.isConnected && this.cluster) {
        await this.cluster.close()
        this.cluster = null
        this.bucket = null
        this.isConnected = false
        this.initPromise = null
        console.log('🔌 Disconnected from Couchbase')
      } else {
        console.log('ℹ️ Not connected to Couchbase, nothing to disconnect')
      }
    } catch (error) {
      console.error('❌ Error disconnecting from Couchbase:', error)
      throw error
    }
  }

  /**
   * Get cluster information
   */
  async getClusterInfo(): Promise<string> {
    if (!this.cluster) {
      throw new Error('Not connected to cluster')
    }

    try {
      const diagnostics = await this.cluster.diagnostics()
      return `Connected to cluster (${JSON.stringify(diagnostics)})`
    } catch (error) {
      console.error('❌ Error getting cluster info:', error)
      throw error
    }
  }

  /**
   * Get the bucket instance (auto-connects if needed)
   */
  async getBucket(): Promise<Bucket> {
    await this.connect()
    if (!this.bucket) {
      throw new Error('Failed to get bucket after connection')
    }
    return this.bucket
  }

  /**
   * Get the cluster instance (auto-connects if needed)
   */
  async getCluster(): Promise<Cluster> {
    await this.connect()
    if (!this.cluster) {
      throw new Error('Failed to get cluster after connection')
    }
    return this.cluster
  }

  /**
   * Get configuration (without sensitive data)
   */
  getConfig(): Omit<CouchbaseConfig, 'password'> {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { password, ...safeConfig } = this.config
    return safeConfig
  }

  /**
   * Check if the client is connected
   */
  isClientConnected(): boolean {
    return this.isConnected
  }

  /**
   * Reset the singleton instance (useful for testing)
   */
  public static resetInstance(): void {
    if (CouchbaseClient.instance) {
      CouchbaseClient.instance.disconnect().catch(() => {
        // Ignore disconnect errors during reset
      })
      CouchbaseClient.instance = null
    }
  }
}

export const client = CouchbaseClient.getInstance()
