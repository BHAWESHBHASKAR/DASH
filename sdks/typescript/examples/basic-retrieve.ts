/**
 * Basic retrieve example.
 *
 * Run with:
 *
 *     npx tsx examples/basic-retrieve.ts
 */

import { createClient } from '../src/index.js';

async function main() {
  const client = createClient({ baseUrl: 'http://localhost:8080' });

  // Default: balanced mode, top_k = 10.
  const response = await client.retrieve.query({
    tenant_id: 'acme-corp',
    query: 'what was the Q3 2024 revenue?',
  });

  console.log(`Got ${response.results.length} claims.`);
  for (const claim of response.results) {
    console.log(`[${claim.score.toFixed(2)}] ${claim.canonical_text}`);
    console.log(`   supports=${claim.supports}  contradicts=${claim.contradicts}`);
    for (const cite of claim.citations) {
      console.log(
        `   - ${cite.stance.padStart(11)}  ${cite.source_id}  (q=${cite.source_quality.toFixed(2)})`,
      );
    }
  }

  // The convenience helper returns just the top result (or null).
  const top = await client.retrieve.topResult({
    tenant_id: 'acme-corp',
    query: 'Q3 2024 revenue',
  });
  console.log('\nTop result:', top?.canonical_text ?? '(none)');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
