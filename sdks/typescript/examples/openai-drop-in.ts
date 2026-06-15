/**
 * Drop-in OpenAI compatibility example.
 *
 * Run with:
 *
 *     npm install openai
 *     OPENAI_API_KEY=not-required npx tsx examples/openai-drop-in.ts
 *
 * Demonstrates that the official `openai` npm package can be pointed
 * at a DASH retrieval service with a one-line change: just pass
 * `baseURL: 'http://localhost:8080/v1'`. DASH's `/v1/embeddings`
 * endpoint is byte-for-byte compatible with OpenAI's spec, so
 * LangChain, LlamaIndex, the OpenAI CLI, and any other tool that
 * respects the `baseURL` knob also work.
 */

import OpenAI from 'openai';
import { openAIBaseURL } from '../src/openai-compat.js';

async function main() {
  const client = new OpenAI({
    baseURL: openAIBaseURL('http://localhost:8080'),
    apiKey: 'not-required-for-local',
  });

  const resp = await client.embeddings.create({
    input: 'hello world',
    model: 'text-embedding-3-small',
  });

  console.log('embeddings.create() through the official openai SDK:');
  console.log('  model:', resp.model);
  console.log('  first 5 dims:', resp.data[0]!.embedding.slice(0, 5));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
