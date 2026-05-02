import { NPlusOneGuard } from './nplusOneQuery.js';

export class MetricsReporter {
  private readonly guard: NPlusOneGuard;
  private readonly intervalMs: number;
  private readonly sink: (report: MetricsReport) => void;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    guard: NPlusOneGuard,
    opts: {
      intervalMs?: number;
      sink?: (report: MetricsReport) => void;
    } = {}
  ) {
    this.guard       = guard;
    this.intervalMs  = opts.intervalMs ?? 10_000;
    this.sink        = opts.sink ?? defaultConsoleSink;
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      const m = this.guard.getMetrics();
      this.sink({
        timestamp:        new Date().toISOString(),
        queriesSaved:     m.queriesSaved,
        batchesExecuted:  m.batchesExecuted,
        detectedPatterns: [...m.detectedPatterns],
      });
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  report(): MetricsReport {
    const m = this.guard.getMetrics();
    return {
      timestamp:        new Date().toISOString(),
      queriesSaved:     m.queriesSaved,
      batchesExecuted:  m.batchesExecuted,
      detectedPatterns: [...m.detectedPatterns],
    };
  }
}

export interface MetricsReport {
  timestamp: string;
  queriesSaved: number;
  batchesExecuted: number;
  detectedPatterns: string[];
}

function defaultConsoleSink(report: MetricsReport): void {
  console.log(
    `[NPlusOneGuard Metrics] ${report.timestamp}` +
    ` | queriesSaved=${report.queriesSaved}` +
    ` | batchesExecuted=${report.batchesExecuted}` +
    ` | patterns=${report.detectedPatterns.join(', ') || 'none'}`
  );
}
