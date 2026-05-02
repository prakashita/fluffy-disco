import {
  mergeUnion,
  mergeSum,
  mergeCount,
  mergeAvg,
  mergeTopK,
  mergeSort,
  ShardPayload,
} from '../src/lib/mergeStrategies';
import { ScatterGatherCoordinator } from '../src/lib/scatterGatherCoordinator';
import { ShardCoordinator, ShardConfig } from '../src/index';

interface OrderRow {
  id: number;
  amount: number;
  userId: string;
}

const shards: ShardConfig[] = [
  { id: 'shard_1', host: 'localhost', port: 5432, database: 'db1', user: 'u', password: 'p' },
  { id: 'shard_2', host: 'localhost', port: 5433, database: 'db2', user: 'u', password: 'p' },
  { id: 'shard_3', host: 'localhost', port: 5434, database: 'db3', user: 'u', password: 'p' },
];

function makePayloads<T>(data: Array<[string, T[]]>): ShardPayload<T>[] {
  return data.map(([shardId, rows]) => ({ shardId, rows }));
}

// ---------------------------------------------------------------------------
// mergeStrategies — unit tests
// ---------------------------------------------------------------------------

describe('mergeUnion', () => {
  it('returns empty array when no payloads provided', () => {
    expect(mergeUnion([])).toEqual([]);
  });

  it('returns flat array of all rows in shard order', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 100, userId: 'u1' }]],
      ['shard_2', [{ id: 2, amount: 200, userId: 'u2' }, { id: 3, amount: 300, userId: 'u3' }]],
    ]);
    expect(mergeUnion(payloads)).toEqual([
      { id: 1, amount: 100, userId: 'u1' },
      { id: 2, amount: 200, userId: 'u2' },
      { id: 3, amount: 300, userId: 'u3' },
    ]);
  });

  it('handles payloads where some shards returned no rows', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 100, userId: 'u1' }]],
      ['shard_2', []],
    ]);
    expect(mergeUnion(payloads)).toEqual([{ id: 1, amount: 100, userId: 'u1' }]);
  });

  it('preserves row objects by reference', () => {
    const row = { id: 1, amount: 50, userId: 'u1' };
    const payloads = makePayloads<OrderRow>([['shard_1', [row]]]);
    expect(mergeUnion(payloads)[0]).toBe(row);
  });
});

describe('mergeSum', () => {
  it('returns 0 for empty payloads', () => {
    expect(mergeSum([], 'amount')).toBe(0);
  });

  it('sums numeric field across all shards correctly', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 200_000, userId: 'u1' }]],
      ['shard_2', [{ id: 2, amount: 300_000, userId: 'u2' }]],
      ['shard_3', [{ id: 3, amount: 500_000, userId: 'u3' }]],
    ]);
    expect(mergeSum(payloads, 'amount')).toBe(1_000_000);
  });

  it('handles shards with zero rows contributing 0 to sum', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 100, userId: 'u1' }]],
      ['shard_2', []],
    ]);
    expect(mergeSum(payloads, 'amount')).toBe(100);
  });

  it('handles field values of 0 (not skipped)', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 0, userId: 'u1' }, { id: 2, amount: 50, userId: 'u2' }]],
    ]);
    expect(mergeSum(payloads, 'amount')).toBe(50);
  });
});

describe('mergeCount', () => {
  it('returns 0 for empty payloads', () => {
    expect(mergeCount([])).toBe(0);
  });

  it('counts total rows across all shards', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 10, userId: 'u1' }, { id: 2, amount: 20, userId: 'u2' }]],
      ['shard_2', [{ id: 3, amount: 30, userId: 'u3' }]],
    ]);
    expect(mergeCount(payloads)).toBe(3);
  });

  it('counts correctly when some shards return empty row arrays', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 10, userId: 'u1' }]],
      ['shard_2', []],
      ['shard_3', [{ id: 3, amount: 30, userId: 'u3' }]],
    ]);
    expect(mergeCount(payloads)).toBe(2);
  });
});

describe('mergeAvg', () => {
  it('returns 0 when no rows (avoids division by zero / NaN)', () => {
    expect(mergeAvg([], 'amount')).toBe(0);
  });

  it('returns 0 for payloads with all-empty row arrays', () => {
    const payloads = makePayloads<OrderRow>([['shard_1', []], ['shard_2', []]]);
    expect(mergeAvg(payloads, 'amount')).toBe(0);
  });

  it('computes correct average when all shards have equal row count', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 100, userId: 'u1' }]],
      ['shard_2', [{ id: 2, amount: 200, userId: 'u2' }]],
    ]);
    expect(mergeAvg(payloads, 'amount')).toBe(150);
  });

  it('computes correct weighted average across shards with unequal row counts', () => {
    // shard_1: 3 rows at amount 100 → sum 300
    // shard_2: 1 row  at amount 200 → sum 200
    // total: 500 / 4 = 125
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [
        { id: 1, amount: 100, userId: 'u1' },
        { id: 2, amount: 100, userId: 'u2' },
        { id: 3, amount: 100, userId: 'u3' },
      ]],
      ['shard_2', [{ id: 4, amount: 200, userId: 'u4' }]],
    ]);
    expect(mergeAvg(payloads, 'amount')).toBe(125);
  });
});

describe('mergeTopK', () => {
  const payloads = makePayloads<OrderRow>([
    ['shard_1', [
      { id: 1, amount: 500, userId: 'u1' },
      { id: 2, amount: 300, userId: 'u2' },
    ]],
    ['shard_2', [
      { id: 3, amount: 450, userId: 'u3' },
      { id: 4, amount: 250, userId: 'u4' },
    ]],
  ]);

  it('returns empty array for empty payloads', () => {
    expect(mergeTopK([], 'amount', 3)).toEqual([]);
  });

  it('returns top K rows sorted descending by default', () => {
    const result = mergeTopK(payloads, 'amount', 2);
    expect(result.map((r) => r.amount)).toEqual([500, 450]);
  });

  it('returns top K rows sorted ascending when order=asc', () => {
    const result = mergeTopK(payloads, 'amount', 2, 'asc');
    expect(result.map((r) => r.amount)).toEqual([250, 300]);
  });

  it('returns all rows when k >= total row count', () => {
    const result = mergeTopK(payloads, 'amount', 100);
    expect(result).toHaveLength(4);
  });

  it('returns exactly k rows when k < total row count', () => {
    const result = mergeTopK(payloads, 'amount', 3);
    expect(result).toHaveLength(3);
  });

  it('returns empty array when k=0', () => {
    expect(mergeTopK(payloads, 'amount', 0)).toEqual([]);
  });

  it('returns single top row when k=1', () => {
    const result = mergeTopK(payloads, 'amount', 1);
    expect(result).toHaveLength(1);
    expect(result[0].amount).toBe(500);
  });
});

describe('mergeSort', () => {
  it('returns empty array for empty payloads', () => {
    expect(mergeSort([], 'amount')).toEqual([]);
  });

  it('sorts ascending by default', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 300, userId: 'u1' }, { id: 2, amount: 100, userId: 'u2' }]],
      ['shard_2', [{ id: 3, amount: 200, userId: 'u3' }]],
    ]);
    expect(mergeSort(payloads, 'amount').map((r) => r.amount)).toEqual([100, 200, 300]);
  });

  it('sorts descending when order=desc', () => {
    const payloads = makePayloads<OrderRow>([
      ['shard_1', [{ id: 1, amount: 300, userId: 'u1' }, { id: 2, amount: 100, userId: 'u2' }]],
      ['shard_2', [{ id: 3, amount: 200, userId: 'u3' }]],
    ]);
    expect(mergeSort(payloads, 'amount', 'desc').map((r) => r.amount)).toEqual([300, 200, 100]);
  });

  it('does not mutate the original row arrays', () => {
    const rows = [{ id: 1, amount: 200, userId: 'u1' }, { id: 2, amount: 100, userId: 'u2' }];
    const payloads = makePayloads<OrderRow>([['shard_1', rows]]);
    mergeSort(payloads, 'amount');
    expect(rows[0].amount).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// ScatterGatherCoordinator
// ---------------------------------------------------------------------------

describe('ScatterGatherCoordinator — happy path', () => {
  it('calls queryFn once per shard with the correct shardId', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator(shards);
    await coordinator.scatter(queryFn, mergeUnion);
    expect(queryFn).toHaveBeenCalledTimes(3);
    expect(queryFn).toHaveBeenCalledWith('shard_1');
    expect(queryFn).toHaveBeenCalledWith('shard_2');
    expect(queryFn).toHaveBeenCalledWith('shard_3');
  });

  it('shardsQueried contains all shard IDs', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.shardsQueried).toEqual(['shard_1', 'shard_2', 'shard_3']);
  });

  it('shardsSucceeded equals shardsQueried when all succeed', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.shardsSucceeded).toEqual(['shard_1', 'shard_2', 'shard_3']);
  });

  it('errors array is empty when all shards succeed', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.errors).toHaveLength(0);
  });

  it('partialResult is false when all shards succeed', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.partialResult).toBe(false);
  });

  it('passes correct ShardPayload[] to mergeFn and returns merged data', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([{ id: 1, amount: 100, userId: 'u1' }])
      .mockResolvedValueOnce([{ id: 2, amount: 200, userId: 'u2' }])
      .mockResolvedValueOnce([{ id: 3, amount: 300, userId: 'u3' }]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter<OrderRow, number>(queryFn, (payloads) =>
      mergeSum(payloads, 'amount'),
    );
    expect(result.data).toBe(600);
  });
});

describe('ScatterGatherCoordinator — partial failure', () => {
  it('collects data from succeeded shards even when one shard throws', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([{ id: 1, amount: 100, userId: 'u1' }])
      .mockRejectedValueOnce(new Error('shard_2 timeout'))
      .mockResolvedValueOnce([{ id: 3, amount: 300, userId: 'u3' }]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter<OrderRow, OrderRow[]>(queryFn, mergeUnion);
    expect(result.data).toHaveLength(2);
    expect(result.data.map((r) => r.id)).toEqual([1, 3]);
  });

  it('errors array contains ShardError with correct shardId and error message', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error('timeout'))
      .mockResolvedValueOnce([]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].shardId).toBe('shard_2');
    expect(result.errors[0].error.message).toBe('timeout');
  });

  it('shardsSucceeded excludes failed shards', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValueOnce([]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.shardsSucceeded).toEqual(['shard_1', 'shard_3']);
  });

  it('partialResult is true when some shards fail and some succeed', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error('fail'))
      .mockResolvedValueOnce([]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.partialResult).toBe(true);
  });

  it('normalises non-Error rejection reason into an Error object', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce('string reason')
      .mockResolvedValueOnce([]);

    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.errors[0].error).toBeInstanceOf(Error);
    expect(result.errors[0].error.message).toBe('string reason');
  });
});

describe('ScatterGatherCoordinator — total failure', () => {
  it('does not throw — error is contained in CrossShardResult', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    await expect(coordinator.scatter(queryFn, mergeUnion)).resolves.toBeDefined();
  });

  it('returns empty merged result when all shards fail (union)', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter<OrderRow, OrderRow[]>(queryFn, mergeUnion);
    expect(result.data).toEqual([]);
  });

  it('returns 0 merged result when all shards fail (sum)', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter<OrderRow, number>(queryFn, (p) =>
      mergeSum(p, 'amount'),
    );
    expect(result.data).toBe(0);
  });

  it('errors array has an entry for every shard', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.errors).toHaveLength(3);
    expect(result.errors.map((e) => e.shardId)).toEqual(['shard_1', 'shard_2', 'shard_3']);
  });

  it('shardsSucceeded is empty when all shards fail', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.shardsSucceeded).toEqual([]);
  });

  it('partialResult is false when no shards succeeded', async () => {
    const queryFn = jest.fn().mockRejectedValue(new Error('all down'));
    const coordinator = new ScatterGatherCoordinator(shards);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.partialResult).toBe(false);
  });
});

describe('ScatterGatherCoordinator — edge cases', () => {
  it('works correctly with a single shard', async () => {
    const queryFn = jest.fn().mockResolvedValue([{ id: 1, amount: 42, userId: 'u1' }]);
    const coordinator = new ScatterGatherCoordinator([shards[0]]);
    const result = await coordinator.scatter<OrderRow, number>(queryFn, (p) =>
      mergeSum(p, 'amount'),
    );
    expect(result.data).toBe(42);
    expect(result.shardsQueried).toEqual(['shard_1']);
  });

  it('works correctly with an empty shards array', async () => {
    const queryFn = jest.fn();
    const coordinator = new ScatterGatherCoordinator([]);
    const result = await coordinator.scatter<OrderRow, OrderRow[]>(queryFn, mergeUnion);
    expect(result.data).toEqual([]);
    expect(result.shardsQueried).toEqual([]);
    expect(queryFn).not.toHaveBeenCalled();
  });

  it('queryFn returning empty array is treated as success with 0 rows', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coordinator = new ScatterGatherCoordinator([shards[0]]);
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.shardsSucceeded).toEqual(['shard_1']);
    expect(result.errors).toHaveLength(0);
  });

  it('queryFn throwing synchronously is caught and treated as rejected', async () => {
    const queryFn = jest.fn().mockImplementation(() => {
      throw new Error('sync error');
    });
    const coordinator = new ScatterGatherCoordinator([shards[0]]);
    // Promise.allSettled wraps sync throws when passed a non-promise as a rejection
    // But queryFn throws before returning, so the promise rejects immediately
    const result = await coordinator.scatter(queryFn, mergeUnion);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].error.message).toBe('sync error');
  });
});

// ---------------------------------------------------------------------------
// ShardCoordinator.scatter() — integration
// ---------------------------------------------------------------------------

describe('ShardCoordinator.scatter()', () => {
  it('scatter() method exists on ShardCoordinator', () => {
    const coord = new ShardCoordinator({ shards });
    expect(typeof coord.scatter).toBe('function');
  });

  it('delegates to ScatterGatherCoordinator and returns CrossShardResult', async () => {
    const queryFn = jest.fn().mockResolvedValue([{ id: 1, amount: 100, userId: 'u1' }]);
    const coord = new ShardCoordinator({ shards });
    const result = await coord.scatter<OrderRow, OrderRow[]>(queryFn, mergeUnion);
    expect(result).toHaveProperty('data');
    expect(result).toHaveProperty('shardsQueried');
    expect(result).toHaveProperty('shardsSucceeded');
    expect(result).toHaveProperty('errors');
    expect(result).toHaveProperty('partialResult');
  });

  it('passes shard IDs from ShardConfig to queryFn', async () => {
    const queryFn = jest.fn().mockResolvedValue([]);
    const coord = new ShardCoordinator({ shards });
    await coord.scatter(queryFn, mergeUnion);
    const calledWith = queryFn.mock.calls.map((c) => c[0]).sort();
    expect(calledWith).toEqual(['shard_1', 'shard_2', 'shard_3']);
  });

  it('scatter() with mergeCount returns total row count across all shards', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([{ id: 1, amount: 10, userId: 'u1' }, { id: 2, amount: 20, userId: 'u2' }])
      .mockResolvedValueOnce([{ id: 3, amount: 30, userId: 'u3' }])
      .mockResolvedValueOnce([]);

    const coord = new ShardCoordinator({ shards });
    const result = await coord.scatter<OrderRow, number>(queryFn, mergeCount);
    expect(result.data).toBe(3);
  });

  it('partial failure propagates errors without throwing', async () => {
    const queryFn = jest.fn()
      .mockResolvedValueOnce([{ id: 1, amount: 10, userId: 'u1' }])
      .mockRejectedValueOnce(new Error('shard down'))
      .mockResolvedValueOnce([{ id: 3, amount: 30, userId: 'u3' }]);

    const coord = new ShardCoordinator({ shards });
    const result = await coord.scatter<OrderRow, OrderRow[]>(queryFn, mergeUnion);
    expect(result.errors).toHaveLength(1);
    expect(result.partialResult).toBe(true);
    expect(result.data).toHaveLength(2);
  });
});
