export interface QueryMeta {
  table: string;
  column: string;
  key: string | number;
  timestamp: number;
  resolve: (result: unknown) => void;
  reject: (err: unknown) => void;
}

export interface QuerySignature {
  id: string;
  table: string;
  column: string;
}

export interface BatchEntry {
  keys: Array<string | number>;
  resolvers: Map<string | number, Array<(result: unknown) => void>>;
  rejecters: Map<string | number, Array<(err: unknown) => void>>;
  timer: ReturnType<typeof setTimeout> | null;
  createdAt: number;
}

export type BatchExecutorFn<TKey extends string | number, TResult> = (
  keys: TKey[]
) => Promise<Map<TKey, TResult>>;

export interface GuardOptions {
  windowMs?: number;
  maxBatchSize?: number;
  detectionThreshold?: number;
  onDetected?: (sig: QuerySignature, count: number) => void;
  onBatchExecuted?: (sig: QuerySignature, batchSize: number, durationMs: number) => void;
  debug?: boolean;
}

export interface GuardMetrics {
  queriesSaved: number;
  batchesExecuted: number;
  detectedPatterns: Set<string>;
}
