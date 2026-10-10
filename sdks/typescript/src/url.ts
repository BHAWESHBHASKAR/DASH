/**
 * Removes trailing `/` characters with a linear scan (a `/\/+$/` regex can
 * backtrack quadratically on long runs of slashes).
 */
export function trimTrailingSlashes(value: string): string {
  let end = value.length;
  while (end > 0 && value.charCodeAt(end - 1) === 47 /* '/' */) {
    end -= 1;
  }
  return value.slice(0, end);
}
