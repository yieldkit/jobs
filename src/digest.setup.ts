import { createHash } from 'node:crypto';
import { vi } from 'vite-plus/test';

/**
 * Node's names for the WebCrypto digest algorithms.
 */
const NODE_DIGESTS = new Map([
  ['SHA-1', 'sha1'],
  ['SHA-256', 'sha256'],
  ['SHA-384', 'sha384'],
  ['SHA-512', 'sha512'],
]);

/**
 * The original `digest`, kept before the spy replaces it, for algorithms Node's `createHash` does
 * not cover here.
 */
const realDigest = crypto.subtle.digest.bind(crypto.subtle);

/**
 * Effect hashes every workflow execution id and activity key with `crypto.subtle.digest`, which
 * runs on libuv's thread pool and settles in real time. While a workflow waits for that hash,
 * `TestClock` counts it as suspended and keeps advancing, so on a busy runner (many test files at
 * once on Linux CI) the workflow resumes at a later test time and its retries drift a step late.
 * Hashing synchronously and returning a settled promise keeps the order of events independent of
 * the machine. Unknown algorithms still go to the real implementation.
 */
vi.spyOn(crypto.subtle, 'digest').mockImplementation((algorithm, data) => {
  const name = typeof algorithm === 'string' ? algorithm : algorithm.name;
  const nodeName = NODE_DIGESTS.get(name.toUpperCase());
  if (nodeName === undefined) {
    return realDigest(algorithm, data);
  }

  const bytes = ArrayBuffer.isView(data)
    ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    : new Uint8Array(data);
  const digest = createHash(nodeName).update(bytes).digest();
  return Promise.resolve(
    digest.buffer.slice(digest.byteOffset, digest.byteOffset + digest.byteLength),
  );
});
