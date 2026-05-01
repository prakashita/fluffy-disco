import {
  ConsistencyCoordinator,
  ConsistencyPolicy,
  ReadResult,
} from './consistencyCoordinator.js';

export interface ConsistencyOperation<T> {
  sessionId?: string;
  keys?: string[];
  table?: string;
  shards: { shardId: string; read: () => Promise<{ data: T; version: number }> }[];
}

export async function withConsistency<T>(
  coordinator: ConsistencyCoordinator,
  policy: ConsistencyPolicy,
  operation: ConsistencyOperation<T>,
): Promise<ReadResult<T>[]> {
  const prevConfig = coordinator.getConfig();
  const override = { ...prevConfig, defaultPolicy: policy };
  coordinator.updateConfig(override);

  try {
    return await coordinator.coordinateRead<T>(
      {
        sessionId: operation.sessionId,
        keys: operation.keys,
        table: operation.table,
      },
      operation.shards,
    );
  } finally {
    coordinator.updateConfig(prevConfig);
  }
}
