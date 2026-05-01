interface PendingWrite {
  key: string;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  promise: Promise<unknown>;
}

export class ReadBarrier {
  private pending: Map<string, PendingWrite[]> = new Map();

  registerWrite(key: string): { writeId: string; complete: (value?: unknown) => void; fail: (err: unknown) => void } {
    const writeId = `${key}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    let resolve!: (value: unknown) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<unknown>((res, rej) => { resolve = res; reject = rej; });

    const entry: PendingWrite = { key, resolve, reject, promise };
    const list = this.pending.get(key) ?? [];
    list.push(entry);
    this.pending.set(key, list);

    return {
      writeId,
      complete: (value?: unknown) => {
        resolve(value);
        this.removePending(key, entry);
      },
      fail: (err: unknown) => {
        reject(err);
        this.removePending(key, entry);
      },
    };
  }

  async waitForKey(key: string, timeoutMs = 1000): Promise<void> {
    const list = this.pending.get(key);
    if (!list || list.length === 0) return;

    const promises = list.map((p) => p.promise);

    await Promise.race([
      Promise.allSettled(promises),
      new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
    ]);
  }

  async waitForKeys(keys: string[], timeoutMs = 1000): Promise<void> {
    await Promise.all(keys.map((k) => this.waitForKey(k, timeoutMs)));
  }

  hasPendingWrites(key: string): boolean {
    const list = this.pending.get(key);
    return !!list && list.length > 0;
  }

  pendingCount(key: string): number {
    return this.pending.get(key)?.length ?? 0;
  }

  private removePending(key: string, entry: PendingWrite): void {
    const list = this.pending.get(key);
    if (!list) return;
    const idx = list.indexOf(entry);
    if (idx !== -1) list.splice(idx, 1);
    if (list.length === 0) this.pending.delete(key);
  }
}
