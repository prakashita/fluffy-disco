import {
  BatchExecutorFn,
  GuardMetrics,
  GuardOptions,
  QuerySignature,
} from './types.js';
import { DetectionEngine } from './detectionEngine.js';
import { BatchQueue } from './batchQueue.js';

const DEFAULTS: Required<GuardOptions> = {
  windowMs:           5,
  maxBatchSize:       100,
  detectionThreshold: 3,
  onDetected:         undefined as unknown as Required<GuardOptions>['onDetected'],
  onBatchExecuted:    undefined as unknown as Required<GuardOptions>['onBatchExecuted'],
  debug:              false,
};

export class NPlusOneGuard {
  private readonly opts: Required<GuardOptions>;
  private readonly detection: DetectionEngine;
  private readonly queues: Map<string, BatchQueue<any, any>> = new Map();
  private readonly executors: Map<string, BatchExecutorFn<any, any>> = new Map();

  readonly metrics: GuardMetrics = {
    queriesSaved:      0,
    batchesExecuted:   0,
    detectedPatterns:  new Set(),
  };

  constructor(opts: GuardOptions = {}) {
    this.opts = { ...DEFAULTS, ...opts };

    this.detection = new DetectionEngine({
      windowMs:           this.opts.windowMs,
      detectionThreshold: this.opts.detectionThreshold,
      debug:              this.opts.debug,
      onDetected: (sig, count) => {
        this.metrics.detectedPatterns.add(sig.id);
        this.opts.onDetected?.(sig, count);
      },
    });
  }

  register<TKey extends string | number, TResult>(
    table: string,
    column: string,
    executor: BatchExecutorFn<TKey, TResult>
  ): void {
    const sigId = buildSigId(table, column);
    if (this.executors.has(sigId)) {
      throw new Error(
        `[NPlusOneGuard] executor already registered for "${sigId}". ` +
        `Call unregister() first if you need to replace it.`
      );
    }
    this.executors.set(sigId, executor);
  }

  unregister(table: string, column: string): void {
    const sigId = buildSigId(table, column);
    this.executors.delete(sigId);
    this.queues.delete(sigId);
  }

  async load<TKey extends string | number, TResult>(
    table: string,
    column: string,
    key: TKey
  ): Promise<TResult> {
    const sig: QuerySignature = {
      id: buildSigId(table, column),
      table,
      column,
    };

    this.detection.record(sig);

    let queue = this.queues.get(sig.id) as BatchQueue<TKey, TResult> | undefined;

    if (!queue) {
      const executor = this.executors.get(sig.id) as
        | BatchExecutorFn<TKey, TResult>
        | undefined;

      if (!executor) {
        throw new Error(
          `[NPlusOneGuard] no executor registered for "${sig.id}". ` +
          `Call guard.register("${table}", "${column}", fn) first.`
        );
      }

      queue = new BatchQueue<TKey, TResult>(
        sig,
        executor,
        {
          windowMs:        this.opts.windowMs,
          maxBatchSize:    this.opts.maxBatchSize,
          debug:           this.opts.debug,
          onBatchExecuted: this.opts.onBatchExecuted,
        },
        this.metrics
      );

      this.queues.set(sig.id, queue);
    }

    return queue.enqueue(key);
  }

  async loadMany<TKey extends string | number, TResult>(
    table: string,
    column: string,
    keys: TKey[]
  ): Promise<TResult[]> {
    return Promise.all(keys.map(k => this.load<TKey, TResult>(table, column, k)));
  }

  getMetrics(): Readonly<GuardMetrics> {
    return { ...this.metrics, detectedPatterns: new Set(this.metrics.detectedPatterns) };
  }

  resetDetection(): void {
    this.detection.reset();
  }
}

function buildSigId(table: string, column: string): string {
  return `${table}:${column}`;
}
