/**
 * Claim + Evidence + Contradiction RAG example.
 *
 * This is the differentiator that makes DASH a real retrieval
 * engine, not just a vector store: every retrieved claim carries
 * explicit support / contradiction tallies and a structured
 * citation list. A RAG pipeline can use that to filter out
 * claims that are contradicted before they ever reach the prompt.
 *
 * Run with:
 *
 *     npx tsx examples/rag-with-citations.ts
 */

import { createClient, DashAPIError, DashConnectionError } from '../src/index.js';
import type { Citation, RetrieveResult } from '../src/index.js';

interface ClaimForPrompt {
  text: string;
  score: number;
  citations: Citation[];
}

async function retrieveCleanClaims(
  query: string,
): Promise<ClaimForPrompt[]> {
  const client = createClient({
    baseUrl: 'http://localhost:8080',
    timeoutMs: 10_000,
  });

  // `support_only` mode drops claims whose contradiction count
  // exceeds their support count, server-side. We additionally
  // filter on the typed response to keep only claims that have at
  // least one supporting citation.
  const response = await client.retrieve.query({
    tenant_id: 'acme-corp',
    query,
    top_k: 10,
    stance_mode: 'support_only',
  });

  return response.results
    .filter((r) => r.supports > 0)
    .map((r) => ({
      text: r.canonical_text,
      score: r.score,
      citations: r.citations,
    }));
}

function renderContext(claims: ClaimForPrompt[]): string {
  return claims
    .map(
      (c) =>
        `- ${c.text} [score=${c.score.toFixed(2)}, ` +
        `citations=${c.citations.length}]`,
    )
    .join('\n');
}

async function main() {
  const query = 'what was the Q3 2024 revenue?';
  let claims: ClaimForPrompt[];
  try {
    claims = await retrieveCleanClaims(query);
  } catch (err) {
    if (err instanceof DashConnectionError) {
      console.error('Could not reach DASH:', err.message);
      console.error('Underlying cause:', err.cause?.message);
      return;
    }
    if (err instanceof DashAPIError) {
      console.error(
        `DASH returned ${err.statusCode} (${err.errorType}):`,
        err.errorMessage,
      );
      return;
    }
    throw err;
  }

  console.log(`Retrieved ${claims.length} supported claim(s):\n`);
  for (const c of claims) {
    console.log(`  • ${c.text}`);
    for (const cite of c.citations) {
      console.log(
        `      ${cite.stance.padStart(11)}  ${cite.source_id}  (q=${cite.source_quality.toFixed(2)})`,
      );
    }
  }

  const context = renderContext(claims);
  console.log('\n--- context block to paste into your prompt ---');
  console.log(context);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
