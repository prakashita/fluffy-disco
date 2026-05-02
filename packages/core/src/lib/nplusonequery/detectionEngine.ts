import { QuerySignature, GuardOptions } from './types.js';

export class DetectionEngine {
  private readonly windowMs: number;
  private readonly threshold: number;
  private readonly onDetected?: (sig: QuerySignature, count: number) => void;
  private readonly debug: boolean;

  private readonly hits: Map<string, number[]> = new Map();
  private readonly reported: Set<string> = new Set();

  constructor(opts: Required<Pick<GuardOptions, 'windowMs' | 'detectionThreshold' | 'debug'>> & {
    onDetected?: GuardOptions['onDetected'];
  }) {
    this.windowMs   = opts.windowMs;
    this.threshold  = opts.detectionThreshold;
    this.onDetected = opts.onDetected;
    this.debug      = opts.debug;
  }

  record(sig: QuerySignature): boolean {
    const now = Date.now();
    let timestamps = this.hits.get(sig.id) ?? [];

    timestamps = timestamps.filter(t => now - t <= this.windowMs);
    timestamps.push(now);
    this.hits.set(sig.id, timestamps);

    const count = timestamps.length;
    const exceeded = count >= this.threshold;

    if (exceeded && !this.reported.has(sig.id)) {
      this.reported.add(sig.id);
      if (this.debug) {
        console.warn(
          `[NPlusOneGuard] N+1 detected — signature="${sig.id}" ` +
          `hit ${count}x within ${this.windowMs}ms`
        );
      }
      this.onDetected?.(sig, count);
    }

    return exceeded;
  }

  getCount(sigId: string): number {
    const now = Date.now();
    const timestamps = (this.hits.get(sigId) ?? []).filter(
      t => now - t <= this.windowMs
    );
    return timestamps.length;
  }

  reset(): void {
    this.hits.clear();
    this.reported.clear();
  }
}
