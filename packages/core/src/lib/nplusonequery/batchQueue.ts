import {
  BatchEntry,
  BatchExecutorFn,
  GuardMetrics,
  GuardOptions,
  QuerySignature,
} from './types.js';

export class BatchQueue<TKey extends string | number, TResult> {
  private readonly sig: QuerySignature;
  private readonly executor: BatchExecutorFn<TKey, TResult>;
  private readonly windowMs: number;
  private readonly maxBatchSize: number;
  private readonly onBatchExecuted?: GuardOptions['onBatchExecuted'];
  private readonly debug: boolean;
  private readonly metrics: GuardMetrics;

  private current: BatchEntry | null = null;

  constructor(
    sig: QuerySignature,
    executor: BatchExecutorFn<TKey, TResult>,
    opts: Required<Pick<GuardOptions, 'windowMs' | 'maxBatchSize' | 'debug'>> & {
      onBatchExecuted?: GuardOptions['onBatchExecuted'];
    },
    metrics: GuardMetrics
  ) {
    this.sig             = sig;
    this.executor        = executor;
    this.windowMs        = opts.windowMs;
    this.maxBatchSize    = opts.maxBatchSize;
    this.onBatchExecuted = opts.onBatchExecuted;
    this.debug           = opts.debug;
    this.metrics         = metrics;
  }

  enqueue(key: TKey): Promise<TResult> {
    return new Promise<TResult>((resolve, reject) => {
      if (!this.current) {
        this.current = {
          keys: [],
          resolvers: new Map(),
          rejecters: new Map(),
          timer: null,
          createdAt: Date.now(),
        };

        this.current.timer = setTimeout(() => this.flush(), this.windowMs);
      }

      const entry = this.current;

      entry.keys.push(key);

      if (!entry.resolvers.has(key)) {
        entry.resolvers.set(key, []);
        entry.rejecters.set(key, []);
      }
      entry.resolvers.get(key)!.push(resolve as (v: unknown) => void);
      entry.rejecters.get(key)!.push(reject);

      if (this.debug) {
        console.debug(
          `[NPlusOneGuard] enqueue key=${key} sig="${this.sig.id}" ` +
          `(batch size=${entry.keys.length})`
        );
      }

      if (entry.keys.length >= this.maxBatchSize) {
        this.flush();
      }
    });
  }

  private flush(): void {
    const entry = this.current;
    if (!entry) return;

    this.current = null;
    if (entry.timer) clearTimeout(entry.timer);

    const uniqueKeys = [...new Set(entry.keys)] as TKey[];
    const start = Date.now();

    if (this.debug) {
      console.debug(
        `[NPlusOneGuard] flushing batch sig="${this.sig.id}" keys=[${uniqueKeys.join(',')}]`
      );
    }

    this.executor(uniqueKeys)
      .then(resultMap => {
        const durationMs = Date.now() - start;

        this.metrics.batchesExecuted += 1;
        this.metrics.queriesSaved += entry.keys.length - 1;

        this.onBatchExecuted?.(this.sig, uniqueKeys.length, durationMs);

        if (this.debug) {
          console.debug(
            `[NPlusOneGuard] batch done sig="${this.sig.id}" ` +
            `keys=${uniqueKeys.length} duration=${durationMs}ms`
          );
        }

        for (const key of entry.keys) {
          const result = resultMap.get(key as TKey);
          const resolvers = entry.resolvers.get(key) ?? [];
          for (const resolve of resolvers) resolve(result);
        }
      })
      .catch(err => {
        for (const rejecterList of entry.rejecters.values()) {
          for (const reject of rejecterList) reject(err);
        }
      });
  }
}
