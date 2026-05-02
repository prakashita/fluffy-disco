/**
 * nplusone-example.ts  –  demonstrates NPlusOneGuard end-to-end
 *
 * Simulates:
 *   1. Fetching 5 users
 *   2. Naively loading each user's posts one-by-one  (classic N+1)
 *   3. The guard transparently batches all 5 post lookups into ONE query
 */

import { NPlusOneGuard, MetricsReporter } from '../src/lib/nplusonequery/index';

interface User { id: number; name: string }
interface Post { id: number; userId: number; title: string }

const USERS: User[] = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Carol' },
  { id: 4, name: 'Dave' },
  { id: 5, name: 'Eve' },
];

const POSTS: Post[] = [
  { id: 10, userId: 1, title: 'Alice post A' },
  { id: 11, userId: 1, title: 'Alice post B' },
  { id: 12, userId: 2, title: 'Bob post A' },
  { id: 13, userId: 3, title: 'Carol post A' },
  { id: 14, userId: 4, title: 'Dave post A' },
  { id: 15, userId: 5, title: 'Eve post A' },
  { id: 16, userId: 5, title: 'Eve post B' },
];

let queryCount = 0;

async function dbFetchPostsByUserIds(userIds: number[]): Promise<Map<number, Post[]>> {
  queryCount++;
  console.log(`  [DB] SELECT * FROM posts WHERE user_id IN (${userIds.join(',')})`);

  await new Promise(r => setTimeout(r, 2));

  const result = new Map<number, Post[]>();
  for (const id of userIds) {
    result.set(id, POSTS.filter(p => p.userId === id));
  }
  return result;
}

async function main() {
  const guard = new NPlusOneGuard({
    windowMs:           5,
    maxBatchSize:       50,
    detectionThreshold: 3,
    debug:              true,
    onDetected: (sig, count) => {
      console.warn(`\nN+1 pattern detected: "${sig.id}" appeared ${count}x — batching activated!\n`);
    },
    onBatchExecuted: (sig, batchSize, durationMs) => {
      console.log(`\nBatch executed for "${sig.id}": ${batchSize} keys in ${durationMs}ms\n`);
    },
  });

  guard.register<number, Post[]>('posts', 'userId', dbFetchPostsByUserIds);

  const reporter = new MetricsReporter(guard);

  console.log('='.repeat(59));
  console.log('  NPlusOneGuard  --  N+1 demo');
  console.log('='.repeat(59) + '\n');
  console.log('Fetching posts for 5 users "one-by-one" (classic N+1 pattern)...\n');

  queryCount = 0;

  const results = await Promise.all(
    USERS.map(async user => {
      const posts = await guard.load<number, Post[]>('posts', 'userId', user.id);
      return { user: user.name, posts: posts?.map(p => p.title) ?? [] };
    })
  );

  console.log('--- Results ' + '-'.repeat(47));
  for (const r of results) {
    console.log(`  ${r.user}: [${r.posts.join(' | ')}]`);
  }

  console.log('\n--- Stats ' + '-'.repeat(49));
  console.log(`  Actual DB queries fired : ${queryCount}  (would have been ${USERS.length} without guard)`);
  console.log(`  Guard metrics           :`, reporter.report());
  console.log('='.repeat(59) + '\n');

  console.log('loadMany() test -- fetching posts for users [1, 3, 5]...\n');
  queryCount = 0;

  const batch = await guard.loadMany<number, Post[]>('posts', 'userId', [1, 3, 5]);
  console.log('loadMany results:');
  batch.forEach((posts, i) => {
    console.log(`  userId=${[1,3,5][i]} -> ${posts?.map(p => p.title).join(', ')}`);
  });
  console.log(`\n  Actual DB queries fired: ${queryCount}`);
  console.log('='.repeat(59) + '\n');
}

main().catch(console.error);
