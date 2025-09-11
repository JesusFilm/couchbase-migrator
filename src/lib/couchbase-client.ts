import { Cluster, Bucket, Collection } from 'couchbase';
import { env } from './env';

export interface CouchbaseConfig {
  connectionString: string;
  username: string;
  password: string;
  bucketName: string;
  scopeName: string;
  collectionName: string;
  connectionTimeout: number;
  operationTimeout: number;
  useSSL: boolean;
  certPath?: string | undefined;
  keyPath?: string | undefined;
  certAuthPath?: string | undefined;
}

class CouchbaseClient {
  private static instance: CouchbaseClient | null = null;
  private cluster: Cluster | null = null;
  private bucket: Bucket | null = null;
  private collection: Collection | null = null;
  private config: CouchbaseConfig;
  private isConnected: boolean = false;
  private initPromise: Promise<CouchbaseClient> | null = null;

  private constructor(config?: Partial<CouchbaseConfig>) {
    this.config = {
      connectionString: env.COUCHBASE_CONNECTION_STRING,
      username: env.COUCHBASE_USERNAME,
      password: env.COUCHBASE_PASSWORD,
      bucketName: env.COUCHBASE_BUCKET_NAME,
      scopeName: env.COUCHBASE_SCOPE_NAME,
      collectionName: env.COUCHBASE_COLLECTION_NAME,
      connectionTimeout: env.COUCHBASE_CONNECTION_TIMEOUT,
      operationTimeout: env.COUCHBASE_OPERATION_TIMEOUT,
      useSSL: env.COUCHBASE_USE_SSL,
      certPath: env.COUCHBASE_CERT_PATH,
      keyPath: env.COUCHBASE_KEY_PATH,
      certAuthPath: env.COUCHBASE_CERT_AUTH_PATH,
      ...config,
    };
  }

  /**
   * Get the singleton instance of CouchbaseClient
   */
  public static getInstance(
    config?: Partial<CouchbaseConfig>
  ): CouchbaseClient {
    if (!CouchbaseClient.instance) {
      CouchbaseClient.instance = new CouchbaseClient(config);
    }
    return CouchbaseClient.instance;
  }

  /**
   * Ensure client is connected (auto-initializes if needed)
   */
  public async ensureConnected(): Promise<CouchbaseClient> {
    if (this.isConnected) {
      return this;
    }

    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = this.connect().then(() => this);
    await this.initPromise;
    return this;
  }

  /**
   * Connect to Couchbase cluster
   */
  async connect(): Promise<void> {
    try {
      console.log('🔌 Connecting to Couchbase cluster...');
      console.log(`📍 Connection string: ${this.config.connectionString}`);
      console.log(`👤 Username: ${this.config.username}`);
      console.log(`🪣 Bucket: ${this.config.bucketName}`);

      const options = {
        username: this.config.username,
        password: this.config.password,
      };

      // Add SSL configuration if enabled
      if (this.config.useSSL) {
        (options as any).securityConfig = {
          tls: {
            certPath: this.config.certPath,
            keyPath: this.config.keyPath,
            certAuthPath: this.config.certAuthPath,
          },
        };
      }

      this.cluster = await Cluster.connect(
        this.config.connectionString,
        options
      );

      // Get bucket and collection
      this.bucket = this.cluster.bucket(this.config.bucketName);
      this.collection = this.bucket
        .scope(this.config.scopeName)
        .collection(this.config.collectionName);

      this.isConnected = true;
      console.log('✅ Successfully connected to Couchbase!');
      console.log(`📊 Cluster info: ${await this.getClusterInfo()}`);
    } catch (error) {
      console.error('❌ Failed to connect to Couchbase:', error);
      throw error;
    }
  }

  /**
   * Disconnect from Couchbase cluster
   */
  async disconnect(): Promise<void> {
    try {
      if (this.isConnected && this.cluster) {
        await this.cluster.close();
        this.cluster = null;
        this.bucket = null;
        this.collection = null;
        this.isConnected = false;
        this.initPromise = null;
        console.log('🔌 Disconnected from Couchbase');
      } else {
        console.log('ℹ️ Not connected to Couchbase, nothing to disconnect');
      }
    } catch (error) {
      console.error('❌ Error disconnecting from Couchbase:', error);
      throw error;
    }
  }

  /**
   * Get cluster information
   */
  async getClusterInfo(): Promise<string> {
    if (!this.cluster) {
      throw new Error('Not connected to cluster');
    }

    try {
      const diagnostics = await this.cluster.diagnostics();
      return `Connected to cluster (${diagnostics.id})`;
    } catch (error) {
      return 'Unable to retrieve cluster info';
    }
  }

  /**
   * Get the collection instance (auto-connects if needed)
   */
  async getCollection(): Promise<Collection> {
    await this.ensureConnected();
    if (!this.collection) {
      throw new Error('Failed to get collection after connection');
    }
    return this.collection;
  }

  /**
   * Get the bucket instance (auto-connects if needed)
   */
  async getBucket(): Promise<Bucket> {
    await this.ensureConnected();
    if (!this.bucket) {
      throw new Error('Failed to get bucket after connection');
    }
    return this.bucket;
  }

  /**
   * Get the cluster instance (auto-connects if needed)
   */
  async getCluster(): Promise<Cluster> {
    await this.ensureConnected();
    if (!this.cluster) {
      throw new Error('Failed to get cluster after connection');
    }
    return this.cluster;
  }

  /**
   * Test the connection with a simple operation
   */
  async testConnection(): Promise<boolean> {
    try {
      const collection = await this.getCollection();
      const testKey = 'test-connection';
      const testDoc = {
        message: 'Hello Couchbase!',
        timestamp: new Date().toISOString(),
      };

      // Try to upsert a test document
      await collection.upsert(testKey, testDoc);

      // Try to get the test document
      await collection.get(testKey);

      // Clean up the test document
      await collection.remove(testKey);

      console.log('✅ Connection test successful!');
      return true;
    } catch (error) {
      console.error('❌ Connection test failed:', error);
      return false;
    }
  }

  /**
   * Get configuration (without sensitive data)
   */
  getConfig(): Omit<CouchbaseConfig, 'password'> {
    const { password, ...safeConfig } = this.config;
    return safeConfig;
  }

  /**
   * Check if the client is connected
   */
  isClientConnected(): boolean {
    return this.isConnected;
  }

  /**
   * Reset the singleton instance (useful for testing)
   */
  public static resetInstance(): void {
    if (CouchbaseClient.instance) {
      CouchbaseClient.instance.disconnect().catch(() => {
        // Ignore disconnect errors during reset
      });
      CouchbaseClient.instance = null;
    }
  }
}

// Export the singleton instance
export const client = CouchbaseClient.getInstance();
export { CouchbaseClient };
