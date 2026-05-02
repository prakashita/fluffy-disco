import { DetectionEngine } from '../src/lib/nplusonequery/detectionEngine';
import { BatchQueue } from '../src/lib/nplusonequery/batchQueue';
import { NPlusOneGuard } from '../src/lib/nplusonequery/nplusOneQuery';
import { MetricsReporter } from '../src/lib/nplusonequery/metricsReporter';
import { GuardMetrics, QuerySignature } from '../src/lib/nplusonequery/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeSig(table: string, column: string): QuerySignature {
  return { id: `${table}:${column}`, table, column };
}

function makeMetrics(): GuardMetrics {
  return { queriesSaved: 0, batchesExecuted: 0, detectedPatterns: new Set() };
}

// ---------------------------------------------------------------------------
// DetectionEngine
// ---------------------------------------------------------------------------

describe('DetectionEngine', () => {
  beforeEach(() => { jest.useFakeTimers(); });
  afterEach(() => { jest.useRealTimers(); });

  it('returns false below threshold', () => {
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 3, debug: false });
    const sig = makeSig('User', 'id');
    expect(engine.record(sig)).toBe(false);
    expect(engine.record(sig)).toBe(false);
  });

  it('returns true at threshold', () => {
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 3, debug: false });
    const sig = makeSig('User', 'id');
    engine.record(sig);
    engine.record(sig);
    expect(engine.record(sig)).toBe(true);
  });

  it('calls onDetected exactly once per signature', () => {
    const onDetected = jest.fn();
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 3, debug: false, onDetected });
    const sig = makeSig('Post', 'userId');
    engine.record(sig);
    engine.record(sig);
    engine.record(sig); // triggers
    engine.record(sig); // already reported
    expect(onDetected).toHaveBeenCalledTimes(1);
    expect(onDetected).toHaveBeenCalledWith(sig, 3);
  });

  it('getCount returns 0 for unknown signature', () => {
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 3, debug: false });
    expect(engine.getCount('unknown:id')).toBe(0);
  });

  it('getCount returns current window count', () => {
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 10, debug: false });
    const sig = makeSig('T', 'c');
    engine.record(sig);
    engine.record(sig);
    expect(engine.getCount('T:c')).toBe(2);
  });

  it('expires hits outside the rolling window', () => {
    const engine = new DetectionEngine({ windowMs: 100, detectionThreshold: 3, debug: false });
    const sig = makeSig('A', 'b');
    engine.record(sig);
    engine.record(sig);
    jest.advanceTimersByTime(101);
    expect(engine.getCount('A:b')).toBe(0);
  });

  it('reset clears all state', () => {
    const onDetected = jest.fn();
    const engine = new DetectionEngine({ windowMs: 1000, detectionThreshold: 2, debug: false, onDetected });
    const sig = makeSig('X', 'y');
    engine.record(sig);
    engine.record(sig);
    engine.reset();
    expect(engine.getCount('X:y')).toBe(0);
    // After reset, onDetected can fire again
    engine.record(sig);
    engine.record(sig);
    expect(onDetected).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// BatchQueue
// ---------------------------------------------------------------------------

describe('BatchQueue', () => {
  beforeEach(() => { jest.useFakeTimers(); });
  afterEach(() => { jest.useRealTimers(); });

  it('calls executor after windowMs with all queued keys', async () => {
    const sig = makeSig('Post', 'id');
    const executor = jest.fn().mockResolvedValue(new Map([[1, { id: 1 }], [2, { id: 2 }]]));
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 50, maxBatchSize: 100, debug: false }, metrics);

    const p1 = queue.enqueue(1);
    const p2 = queue.enqueue(2);

    jest.advanceTimersByTime(50);
    await Promise.resolve(); // flush microtask queue

    expect(executor).toHaveBeenCalledTimes(1);
    expect(executor).toHaveBeenCalledWith([1, 2]);
    await expect(p1).resolves.toEqual({ id: 1 });
    await expect(p2).resolves.toEqual({ id: 2 });
  });

  it('flushes early when maxBatchSize is reached', async () => {
    const sig = makeSig('T', 'c');
    const resultMap = new Map([[1, 'a'], [2, 'b'], [3, 'c']]);
    const executor = jest.fn().mockResolvedValue(resultMap);
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 9999, maxBatchSize: 3, debug: false }, metrics);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    await Promise.resolve();

    expect(executor).toHaveBeenCalledTimes(1);
  });

  it('deduplicates repeated keys in executor call', async () => {
    const sig = makeSig('U', 'id');
    const executor = jest.fn().mockResolvedValue(new Map([[5, 'alice']]));
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 10, maxBatchSize: 100, debug: false }, metrics);

    const p1 = queue.enqueue(5);
    const p2 = queue.enqueue(5);

    jest.advanceTimersByTime(10);
    await Promise.resolve();

    expect(executor).toHaveBeenCalledWith([5]);
    await expect(p1).resolves.toBe('alice');
    await expect(p2).resolves.toBe('alice');
  });

  it('rejects all waiting promises when executor throws', async () => {
    const sig = makeSig('X', 'y');
    const executor = jest.fn().mockRejectedValue(new Error('db down'));
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 10, maxBatchSize: 100, debug: false }, metrics);

    const p1 = queue.enqueue(1);

    jest.advanceTimersByTime(10);
    await Promise.resolve();

    await expect(p1).rejects.toThrow('db down');
  });

  it('updates metrics after successful batch', async () => {
    const sig = makeSig('M', 'id');
    const executor = jest.fn().mockResolvedValue(new Map([[1, 'x'], [2, 'y'], [3, 'z']]));
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 10, maxBatchSize: 100, debug: false }, metrics);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    jest.advanceTimersByTime(10);
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(metrics.batchesExecuted).toBe(1);
    expect(metrics.queriesSaved).toBe(2); // 3 individual calls - 1 batch = 2 saved
  });

  it('calls onBatchExecuted callback', async () => {
    const sig = makeSig('N', 'id');
    const onBatchExecuted = jest.fn();
    const executor = jest.fn().mockResolvedValue(new Map([[7, 'r']]));
    const metrics = makeMetrics();
    const queue = new BatchQueue(sig, executor, { windowMs: 10, maxBatchSize: 100, debug: false, onBatchExecuted }, metrics);

    queue.enqueue(7);

    jest.advanceTimersByTime(10);
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onBatchExecuted).toHaveBeenCalledTimes(1);
    expect(onBatchExecuted).toHaveBeenCalledWith(sig, 1, expect.any(Number));
  });
});

// ---------------------------------------------------------------------------
// NPlusOneGuard
// ---------------------------------------------------------------------------

describe('NPlusOneGuard', () => {
  beforeEach(() => { jest.useFakeTimers(); });
  afterEach(() => { jest.useRealTimers(); });

  it('register + load returns correct result', async () => {
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, string>('User', 'id', async (ids) => {
      return new Map(ids.map(id => [id, `user-${id}`]));
    });

    const p = guard.load<number, string>('User', 'id', 42);
    jest.advanceTimersByTime(10);
    await Promise.resolve();

    await expect(p).resolves.toBe('user-42');
  });

  it('batches concurrent loads into a single executor call', async () => {
    const executor = jest.fn().mockImplementation(async (ids: number[]) => {
      return new Map(ids.map(id => [id, `u${id}`]));
    });
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, string>('User', 'id', executor);

    const p1 = guard.load<number, string>('User', 'id', 1);
    const p2 = guard.load<number, string>('User', 'id', 2);
    const p3 = guard.load<number, string>('User', 'id', 3);

    jest.advanceTimersByTime(10);
    await Promise.resolve();

    expect(executor).toHaveBeenCalledTimes(1);
    expect(executor).toHaveBeenCalledWith([1, 2, 3]);
    await expect(p1).resolves.toBe('u1');
    await expect(p2).resolves.toBe('u2');
    await expect(p3).resolves.toBe('u3');
  });

  it('throws if load called without registering executor', async () => {
    const guard = new NPlusOneGuard();
    await expect(guard.load('Unknown', 'id', 1)).rejects.toThrow('no executor registered');
  });

  it('throws if register called twice for same signature', () => {
    const guard = new NPlusOneGuard();
    guard.register('T', 'c', async () => new Map());
    expect(() => guard.register('T', 'c', async () => new Map())).toThrow('already registered');
  });

  it('unregister allows re-registering', () => {
    const guard = new NPlusOneGuard();
    guard.register('T', 'c', async () => new Map());
    guard.unregister('T', 'c');
    expect(() => guard.register('T', 'c', async () => new Map())).not.toThrow();
  });

  it('loadMany resolves all keys in order', async () => {
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, number>('N', 'v', async (ids) => new Map(ids.map(id => [id, id * 2])));

    const promise = guard.loadMany<number, number>('N', 'v', [1, 2, 3]);
    jest.advanceTimersByTime(10);
    await Promise.resolve();

    await expect(promise).resolves.toEqual([2, 4, 6]);
  });

  it('getMetrics returns snapshot with correct counts', async () => {
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, string>('P', 'id', async (ids) => new Map(ids.map(id => [id, `p${id}`])));

    guard.load<number, string>('P', 'id', 1);
    guard.load<number, string>('P', 'id', 2);
    guard.load<number, string>('P', 'id', 3);

    jest.advanceTimersByTime(10);
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    const m = guard.getMetrics();
    expect(m.batchesExecuted).toBe(1);
    expect(m.queriesSaved).toBe(2);
  });

  it('getMetrics returns an independent snapshot (not a live reference)', async () => {
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, string>('Q', 'id', async (ids) => new Map(ids.map(id => [id, `q${id}`])));

    const m1 = guard.getMetrics();
    guard.load<number, string>('Q', 'id', 1);
    jest.advanceTimersByTime(10);
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    const m2 = guard.getMetrics();
    expect(m1.batchesExecuted).toBe(0);
    expect(m2.batchesExecuted).toBe(1);
  });

  it('resetDetection clears detected patterns', () => {
    const onDetected = jest.fn();
    const guard = new NPlusOneGuard({ windowMs: 1000, detectionThreshold: 2, debug: false, onDetected });
    guard.register<number, string>('R', 'id', async () => new Map());

    guard.load<number, string>('R', 'id', 1);
    guard.load<number, string>('R', 'id', 2);

    expect(onDetected).toHaveBeenCalledTimes(1);

    guard.resetDetection();
    onDetected.mockClear();

    guard.load<number, string>('R', 'id', 3);
    guard.load<number, string>('R', 'id', 4);

    expect(onDetected).toHaveBeenCalledTimes(1);
  });

  it('onDetected callback receives correct signature and count', () => {
    const onDetected = jest.fn();
    const guard = new NPlusOneGuard({ windowMs: 1000, detectionThreshold: 2, onDetected });
    guard.register<number, string>('S', 'id', async () => new Map());

    guard.load<number, string>('S', 'id', 1);
    guard.load<number, string>('S', 'id', 2);

    expect(onDetected).toHaveBeenCalledWith(
      expect.objectContaining({ id: 'S:id', table: 'S', column: 'id' }),
      2
    );
  });
});

// ---------------------------------------------------------------------------
// MetricsReporter
// ---------------------------------------------------------------------------

describe('MetricsReporter', () => {
  beforeEach(() => { jest.useFakeTimers(); });
  afterEach(() => { jest.useRealTimers(); });

  it('report() returns snapshot from guard', async () => {
    const guard = new NPlusOneGuard({ windowMs: 10 });
    guard.register<number, string>('T', 'id', async (ids) => new Map(ids.map(id => [id, `t${id}`])));

    guard.load<number, string>('T', 'id', 1);
    guard.load<number, string>('T', 'id', 2);

    jest.advanceTimersByTime(10);
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    const reporter = new MetricsReporter(guard);
    const report = reporter.report();

    expect(report.batchesExecuted).toBe(1);
    expect(report.queriesSaved).toBe(1);
    expect(typeof report.timestamp).toBe('string');
    expect(Array.isArray(report.detectedPatterns)).toBe(true);
  });

  it('start() calls sink on interval', () => {
    const guard = new NPlusOneGuard();
    const sink = jest.fn();
    const reporter = new MetricsReporter(guard, { intervalMs: 500, sink });

    reporter.start();
    jest.advanceTimersByTime(1500);
    reporter.stop();

    expect(sink).toHaveBeenCalledTimes(3);
    expect(sink).toHaveBeenCalledWith(expect.objectContaining({
      batchesExecuted: expect.any(Number),
      queriesSaved: expect.any(Number),
      detectedPatterns: expect.any(Array),
      timestamp: expect.any(String),
    }));
  });

  it('start() is idempotent — calling twice does not double the interval', () => {
    const guard = new NPlusOneGuard();
    const sink = jest.fn();
    const reporter = new MetricsReporter(guard, { intervalMs: 500, sink });

    reporter.start();
    reporter.start();
    jest.advanceTimersByTime(1000);
    reporter.stop();

    expect(sink).toHaveBeenCalledTimes(2);
  });

  it('stop() halts interval', () => {
    const guard = new NPlusOneGuard();
    const sink = jest.fn();
    const reporter = new MetricsReporter(guard, { intervalMs: 200, sink });

    reporter.start();
    jest.advanceTimersByTime(200);
    reporter.stop();
    jest.advanceTimersByTime(600);

    expect(sink).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// Integration — NPlusOneGuard via main src/index.ts exports
// ---------------------------------------------------------------------------

describe('NPlusOneGuard via main package exports', () => {
  it('is exported from src/index', async () => {
    const { NPlusOneGuard: Guard } = await import('../src/index');
    expect(Guard).toBeDefined();
    const guard = new Guard();
    expect(typeof guard.register).toBe('function');
    expect(typeof guard.load).toBe('function');
  });

  it('MetricsReporter is exported from src/index', async () => {
    const { MetricsReporter: Reporter } = await import('../src/index');
    expect(Reporter).toBeDefined();
  });
});
