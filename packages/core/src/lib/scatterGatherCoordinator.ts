import { ShardPayload } from './mergeStrategies.js';

export type MergeStrategyFn<T, R> = (payloads: ShardPayload<T>[]) => R;

export interface ShardError {
  shardId: string;
  error: Error;
}

export interface CrossShardResult<T, R = T[]> {
  data: R;
  shardsQueried: string[];
  shardsSucceeded: string[];
  errors: ShardError[];
  partialResult: boolean;
}

export class ScatterGatherCoordinator {
  private readonly shards: Array<{ id: string }>;

  constructor(shards: Array<{ id: string }>) {
    this.shards = shards;
  }

  async scatter<T, R>(
    queryFn: (shardId: string) => Promise<T[]>,
    mergeFn: MergeStrategyFn<T, R>,
  ): Promise<CrossShardResult<T, R>> {
    const tasks = this.shards.map((s) => ({
      shardId: s.id,
      promise: Promise.resolve().then(() => queryFn(s.id)),
    }));
    const settled = await Promise.allSettled(tasks.map((t) => t.promise));

    const successes: ShardPayload<T>[] = [];
    const errors: ShardError[] = [];

    settled.forEach((result, i) => {
      const shardId = tasks[i].shardId;
      if (result.status === 'fulfilled') {
        successes.push({ shardId, rows: result.value });
      } else {
        const reason = result.reason;
        errors.push({
          shardId,
          error: reason instanceof Error ? reason : new Error(String(reason)),
        });
      }
    });

    const data = mergeFn(successes);

    return {
      data,
      shardsQueried: this.shards.map((s) => s.id),
      shardsSucceeded: successes.map((p) => p.shardId),
      errors,
      partialResult: errors.length > 0 && successes.length > 0,
    };
  }
}
