export interface ShardPayload<T> {
  shardId: string;
  rows: T[];
}

export type MergeOptions =
  | { type: 'union' }
  | { type: 'sum'; field: string }
  | { type: 'count' }
  | { type: 'avg'; field: string }
  | { type: 'topK'; field: string; k: number; order?: 'asc' | 'desc' }
  | { type: 'sort'; field: string; order?: 'asc' | 'desc' };

export function mergeUnion<T>(payloads: ShardPayload<T>[]): T[] {
  return payloads.flatMap((p) => p.rows);
}

export function mergeSum<T>(payloads: ShardPayload<T>[], field: string): number {
  const asRecord = (row: T) => (row as Record<string, unknown>)[field];
  return payloads.flatMap((p) => p.rows).reduce((acc, row) => acc + Number(asRecord(row)), 0);
}

export function mergeCount<T>(payloads: ShardPayload<T>[]): number {
  return payloads.reduce((acc, p) => acc + p.rows.length, 0);
}

export function mergeAvg<T>(payloads: ShardPayload<T>[], field: string): number {
  const rows = payloads.flatMap((p) => p.rows);
  if (rows.length === 0) return 0;
  const asRecord = (row: T) => (row as Record<string, unknown>)[field];
  const sum = rows.reduce((acc, row) => acc + Number(asRecord(row)), 0);
  return sum / rows.length;
}

export function mergeTopK<T>(
  payloads: ShardPayload<T>[],
  field: string,
  k: number,
  order: 'asc' | 'desc' = 'desc',
): T[] {
  return mergeSort(payloads, field, order).slice(0, k);
}

export function mergeSort<T>(
  payloads: ShardPayload<T>[],
  field: string,
  order: 'asc' | 'desc' = 'asc',
): T[] {
  const asRecord = (row: T) => (row as Record<string, unknown>)[field];
  const rows = payloads.flatMap((p) => p.rows);
  const dir = order === 'asc' ? 1 : -1;
  return rows.slice().sort((a, b) => dir * (Number(asRecord(a)) - Number(asRecord(b))));
}
