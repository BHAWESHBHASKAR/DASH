/**
 * Drop-in OpenAI compatibility helper.
 *
 * DASH's `/v1/embeddings` endpoint is byte-for-byte compatible with
 * OpenAI's wire format, so the official `openai` npm package can be
 * pointed at DASH with a one-line change:
 *
 *     import OpenAI from 'openai';
 *     import { openAIBaseURL } from 'dash-ts';
 *
 *     const client = new OpenAI({
 *       baseURL: openAIBaseURL('http://localhost:8080'),
 *       apiKey: 'not-required-for-local',
 *     });
 *     const resp = await client.embeddings.create({
 *       input: 'hello world',
 *       model: 'text-embedding-3-small',
 *     });
 *
 * This works with LangChain, LlamaIndex, the `openai` CLI, and any
 * other tool that respects the `OPENAI_BASE_URL` / `baseURL` knob.
 */
import { trimTrailingSlashes } from './url.js';

/**
 * Build the OpenAI-style base URL pointing at a DASH deployment.
 *
 * Accepts either a bare DASH root (`http://localhost:8080`) or an
 * already-`/v1`-suffixed URL and returns a value with exactly one
 * `/v1` suffix.
 */
export function openAIBaseURL(dashBaseUrl: string): string {
  const trimmed = trimTrailingSlashes(dashBaseUrl);
  if (trimmed.endsWith('/v1')) {
    return trimmed;
  }
  return `${trimmed}/v1`;
}

/**
 * Re-export the version string for the package so callers can
 * pin against a known SDK release.
 */
export const DASH_TS_VERSION = '0.2.0';
