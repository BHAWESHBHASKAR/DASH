/**
 * Basic embeddings example.
 *
 * Run with:
 *
 *     npx tsx examples/basic-embeddings.ts
 *
 * Assumes a DASH retrieval service is reachable at
 * http://localhost:8080. Start one with:
 *
 *     cargo run --release --bin retrieval
 */

import { createClient } from '../src/index.js';

async function main() {
  const client = createClient({ baseUrl: 'http://localhost:8080' });

  // Single string → 1 embedding.
  const single = await client.embeddings.create('hello world');
  console.log('Single:', single.data[0]!.embedding.slice(0, 5));
  console.log('  model:', single.model);
  console.log('  usage:', single.usage);

  // Array of strings → N embeddings, one per input.
  const many = await client.embeddings.create([
    'the quick brown fox',
    'jumps over the lazy dog',
  ]);
  console.log('Many: count =', many.data.length);
  for (const d of many.data) {
    console.log(`  [${d.index}] dim=${d.embedding.length}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
