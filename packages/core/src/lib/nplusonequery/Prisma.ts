import { NPlusOneGuard } from './nplusOneQuery.js';
import { BatchExecutorFn } from './types.js';

export function createPrismaMiddleware(guard: NPlusOneGuard) {
  return async (params: PrismaMiddlewareParams, next: PrismaNextFn) => {
    if (
      params.action !== 'findUnique' &&
      params.action !== 'findFirst'
    ) {
      return next(params);
    }

    const where = (params.args?.where ?? {}) as Record<string, unknown>;
    const keys = Object.keys(where);

    const firstVal = where[keys[0]];
    if (keys.length !== 1 || (typeof firstVal !== 'string' && typeof firstVal !== 'number')) {
      return next(params);
    }

    const column  = keys[0];
    const keyVal  = firstVal as string | number;
    const table   = params.model!;

    try {
      return await guard.load(table, column, keyVal);
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('no executor registered')) {
        return next(params);
      }
      throw err;
    }
  };
}

export interface PrismaMiddlewareParams {
  model?: string;
  action: string;
  args: Record<string, unknown>;
  dataPath: string[];
  runInTransaction: boolean;
}

export type PrismaNextFn = (params: PrismaMiddlewareParams) => Promise<unknown>;

export function registerPrismaLoader<TKey extends string | number, TResult>(
  guard: NPlusOneGuard,
  model: string,
  field: string,
  executor: BatchExecutorFn<TKey, TResult>
): void {
  guard.register<TKey, TResult>(model, field, executor);
}
