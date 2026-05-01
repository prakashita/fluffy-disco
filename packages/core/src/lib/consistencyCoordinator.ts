import { VersionTracker } from './versionTracker.js';
import { ReadBarrier } from './readBarrier.js';

export type ConsistencyPolicy =
  | 'eventual'
  | 'read_after_write'
  | 'bounded'
  | 'causal'
  | 'linearizable';

export interface ConsistencyConfig {
  defaultPolicy: ConsistencyPolicy;
  stalenessBoundMs: number;
  perTable: Record<string, ConsistencyPolicy>;
}

export interface ReadContext {
  sessionId?: string;
  keys?: string[];
  table?: string;
}

export interface ReadResult<T = unknown> {
  data: T;
  version: number;
  shardId: string;
  stale: boolean;
}

export interface ConsistencyViolation {
  type: 'stale_read' | 'version_skew' | 'barrier_timeout';
  details: string;
  timestamp: number;
}

const DEFAULT_CONFIG: ConsistencyConfig = {
  defaultPolicy: 'read_after_write',
  stalenessBoundMs: 1000,
  perTable: {},
};

export class ConsistencyCoordinator {
  readonly versionTracker: VersionTracker;
  readonly readBarrier: ReadBarrier;
  private config: ConsistencyConfig;
  private violations: ConsistencyViolation[] = [];

  constructor(config: Partial<ConsistencyConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.versionTracker = new VersionTracker();
    this.readBarrier = new ReadBarrier();
  }

  resolvePolicy(ctx: ReadContext): ConsistencyPolicy {
    if (ctx.table && this.config.perTable[ctx.table]) {
      return this.config.perTable[ctx.table];
    }
    return this.config.defaultPolicy;
  }

  recordWrite(sessionId: string, shardId: string, key: string, version: number): void {
    this.versionTracker.recordWrite(sessionId, shardId, key, version);
  }

  async coordinateRead<T>(
    ctx: ReadContext,
    shardReaders: { shardId: string; read: () => Promise<{ data: T; version: number }> }[],
  ): Promise<ReadResult<T>[]> {
    const policy = this.resolvePolicy(ctx);

    if (policy !== 'eventual' && ctx.keys) {
      await this.readBarrier.waitForKeys(ctx.keys);
    }

    const results = await Promise.all(
      shardReaders.map(async ({ shardId, read }) => {
        const { data, version } = await read();
        const stale = this.isStale(policy, ctx, shardId, version);

        if (stale) {
          this.violations.push({
            type: 'stale_read',
            details: `shard=${shardId} version=${version} policy=${policy}`,
            timestamp: Date.now(),
          });
        }

        return { data, version, shardId, stale };
      }),
    );

    if (policy === 'linearizable' || policy === 'causal') {
      this.checkVersionSkew(results, policy);
    }

    return results;
  }

  private isStale(policy: ConsistencyPolicy, ctx: ReadContext, shardId: string, version: number): boolean {
    switch (policy) {
      case 'eventual':
        return false;

      case 'read_after_write': {
        if (!ctx.sessionId || !ctx.keys) return false;
        for (const key of ctx.keys) {
          const minVersion = this.versionTracker.getMinimumReadVersion(ctx.sessionId, key);
          if (minVersion > 0 && version < minVersion) return true;
        }
        return false;
      }

      case 'bounded': {
        const minTimestamp = Date.now() - this.config.stalenessBoundMs;
        const expectedVersion = this.versionTracker.getVersionAtTime(shardId, minTimestamp);
        return expectedVersion > 0 && version < expectedVersion;
      }

      case 'causal':
      case 'linearizable': {
        const shardVersion = this.versionTracker.getShardVersion(shardId);
        return shardVersion > 0 && version < shardVersion;
      }

      default:
        return false;
    }
  }

  private checkVersionSkew<T>(results: ReadResult<T>[], policy: ConsistencyPolicy): void {
    if (results.length < 2) return;
    const versions = results.map((r) => r.version);
    const skew = Math.max(...versions) - Math.min(...versions);

    if (policy === 'linearizable' && skew > 0) {
      this.violations.push({
        type: 'version_skew',
        details: `skew=${skew} across ${results.length} shards`,
        timestamp: Date.now(),
      });
    }
  }

  getViolations(): ConsistencyViolation[] {
    return [...this.violations];
  }

  getViolationCount(): number {
    return this.violations.length;
  }

  clearViolations(): void {
    this.violations = [];
  }

  getConfig(): ConsistencyConfig {
    return { ...this.config };
  }

  updateConfig(patch: Partial<ConsistencyConfig>): void {
    this.config = { ...this.config, ...patch };
  }
}
