export interface WriteRecord {
  key: string;
  version: number;
  timestamp: number;
}

export class VersionTracker {
  private shardVersions: Map<string, number> = new Map();
  private keyVersions: Map<string, { shardId: string; version: number; timestamp: number }> = new Map();
  private sessionWrites: Map<string, WriteRecord[]> = new Map();

  recordWrite(sessionId: string, shardId: string, key: string, version: number): void {
    const timestamp = Date.now();

    this.shardVersions.set(
      shardId,
      Math.max(this.shardVersions.get(shardId) ?? 0, version),
    );

    this.keyVersions.set(key, { shardId, version, timestamp });

    const writes = this.sessionWrites.get(sessionId) ?? [];
    writes.push({ key, version, timestamp });
    this.sessionWrites.set(sessionId, writes);
  }

  getMinimumReadVersion(sessionId: string, key: string): number {
    const writes = this.sessionWrites.get(sessionId);
    if (!writes) return 0;
    for (let i = writes.length - 1; i >= 0; i--) {
      if (writes[i].key === key) return writes[i].version;
    }
    return 0;
  }

  getShardVersion(shardId: string): number {
    return this.shardVersions.get(shardId) ?? 0;
  }

  getKeyVersion(key: string): { shardId: string; version: number; timestamp: number } | undefined {
    return this.keyVersions.get(key);
  }

  getSessionWrites(sessionId: string): WriteRecord[] {
    return this.sessionWrites.get(sessionId) ?? [];
  }

  getVersionAtTime(shardId: string, minTimestamp: number): number {
    let maxVersion = 0;
    for (const [, record] of this.keyVersions) {
      if (record.shardId === shardId && record.timestamp >= minTimestamp) {
        maxVersion = Math.max(maxVersion, record.version);
      }
    }
    return maxVersion;
  }

  pruneSessionsBefore(cutoffMs: number): void {
    const cutoff = Date.now() - cutoffMs;
    for (const [sessionId, writes] of this.sessionWrites) {
      const kept = writes.filter((w) => w.timestamp >= cutoff);
      if (kept.length === 0) {
        this.sessionWrites.delete(sessionId);
      } else {
        this.sessionWrites.set(sessionId, kept);
      }
    }
  }
}
