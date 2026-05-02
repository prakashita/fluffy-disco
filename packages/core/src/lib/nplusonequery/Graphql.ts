import { NPlusOneGuard } from './nplusOneQuery.js';
import { BatchExecutorFn } from './types.js';

export function createLoader<TKey extends string | number, TResult>(
  guard: NPlusOneGuard,
  table: string,
  column: string
): (key: TKey) => Promise<TResult> {
  return (key: TKey) => guard.load<TKey, TResult>(table, column, key);
}

export function createLoaderAndRegister<TKey extends string | number, TResult>(
  guard: NPlusOneGuard,
  table: string,
  column: string,
  executor: BatchExecutorFn<TKey, TResult>
): (key: TKey) => Promise<TResult> {
  guard.register<TKey, TResult>(table, column, executor);
  return createLoader<TKey, TResult>(guard, table, column);
}

export function withGuard<TArgs extends unknown[], TReturn>(
  _guard: NPlusOneGuard,
  fn: (...args: TArgs) => Promise<TReturn>
): (...args: TArgs) => Promise<TReturn> {
  return fn;
}
