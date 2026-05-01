import { Client } from 'pg';
import { hotCache } from './lib/hotKeyCache.js';

// Re-export the HotKeyCache library so consumers can use it directly:
// import { HotKeyCache, hotCache, FrequencyCounter } from '@fluffy-disco/core'
export {
  HotKeyCache,
  FrequencyCounter,
  hotCache,
} from './lib/hotKeyCache.js';

export type {
  HotKeyCacheOptions,
  CacheStats,
  CacheHitPayload,
  CacheMissPayload,
  CachePromotedPayload,
  CacheInvalidatedPayload,
  CacheEvictedPayload,
} from './lib/hotKeyCache.js';

export { VersionTracker } from './lib/versionTracker.js';
export type { WriteRecord } from './lib/versionTracker.js';

export { ReadBarrier } from './lib/readBarrier.js';

export { ConsistencyCoordinator } from './lib/consistencyCoordinator.js';
export type {
  ConsistencyPolicy,
  ConsistencyConfig,
  ReadContext,
  ReadResult,
  ConsistencyViolation,
} from './lib/consistencyCoordinator.js';

export { withConsistency } from './lib/withConsistency.js';
export type { ConsistencyOperation } from './lib/withConsistency.js';

export const VERSION = '0.1.0';


export class ShardCoordinator {
  private shardConfig: ShardConfig[];

  constructor(config: ShardCoordinatorOptions) {
    this.shardConfig = config.shards;
    console.log(`[fluffy-disco] Initialized with ${this.shardConfig.length} shards`);
  }

 
  //  Get the current shard configuration.
  getShards(): ShardConfig[] {
    return this.shardConfig;
  }

  
  //  Test connectivity to all configured shards.
  //  Results are served from HotKeyCache for frequently-checked shards;
  //  a failed connection invalidates the cached key immediately.
  async testConnection(): Promise<{ shardId: string; success: boolean; error?: string }[]> {
    const results = await Promise.all(
      this.shardConfig.map(async (shard) => {
        const cacheKey = `shard:${shard.id}`;

        return hotCache.get(cacheKey, async (_key) => {
          const client = new Client({
            host: shard.host,
            port: shard.port,
            user: shard.user,
            password: shard.password,
            database: shard.database,
            connectionTimeoutMillis: 5000,
          });

          try {
            await client.connect();
            await client.query('SELECT 1');
            await client.end();
            return { shardId: shard.id, success: true };
          } catch (error: any) {
            // Invalidate so we don't serve a stale "healthy" result after failure
            hotCache.invalidate(cacheKey);
            return { shardId: shard.id, success: false, error: error.message };
          }
        });
      })
    );

    return results;
  }
}

//  Configuration for a single database shard.
export interface ShardConfig {
  id: string;
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

//  Options for initializing the ShardCoordinator.
export interface ShardCoordinatorOptions {
  shards: ShardConfig[];
}
