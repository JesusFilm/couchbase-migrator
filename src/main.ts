#!/usr/bin/env node

/**
 * Couchbase Migrator - Main Entry Point
 *
 * This is the main entry point for the Couchbase migration tool.
 * Add your migration logic here.
 */

import { client } from '@/lib/couchbase-client';

console.log('🚀 Couchbase Migrator starting...');

// Example function to demonstrate TypeScript usage
function greet(name: string): string {
  return `Hello, ${name}! Welcome to Couchbase Migrator.`;
}

// Main execution
async function main(): Promise<void> {
  const message = greet('Developer');
  console.log(message);

  try {
    // Test the connection (auto-connects if needed)
    const connectionTest = await client.testConnection();

    if (connectionTest) {
      console.log('📦 Ready to perform migrations...');

      // Example: Get cluster info
      const clusterInfo = await client.getClusterInfo();
      console.log(`🔍 ${clusterInfo}`);

      // Example: Access collection for operations
      console.log(
        `📁 Using collection: ${client.getConfig().bucketName}.${
          client.getConfig().scopeName
        }.${client.getConfig().collectionName}`
      );

      // Add your migration logic here
      console.log('✨ Migration framework ready!');
    } else {
      console.error(
        '❌ Connection test failed. Please check your Couchbase configuration.'
      );
    }
  } catch (error) {
    console.error('❌ Error during Couchbase operations:', error);
  } finally {
    // Always disconnect when done (disconnect handles checking if connected)
    try {
      await client.disconnect();
    } catch (disconnectError) {
      console.error('❌ Error disconnecting:', disconnectError);
    }
  }
}

// Run the main function
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  });
}

export { greet, main };
