import { S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { PutObjectCommand } from "@aws-sdk/client-s3";

const MANIFEST_KEY = "_manifest.jsonl";

/**
 * Cloudflare Worker — campaign finance R2 upload gateway.
 *
 * Exposes three authenticated POST endpoints that together implement a
 * two-phase push protocol: clients first declare what they intend to upload
 * (/push/intent), receive pre-signed S3 URLs, upload directly to R2, then
 * confirm the completed push (/push/confirm). Deletions are recorded via
 * /push/delete. All mutations are appended to a NDJSON manifest file
 * (_manifest.jsonl) in the bucket for audit purposes.
 *
 * Authentication: every request must include an X-Api-Key header matching
 * the API_KEY secret bound to this worker.
 *
 * Routes:
 *   POST /push/intent   — declare files to upload, receive signed URLs
 *   POST /push/confirm  — record a completed push in the manifest
 *   POST /push/delete   — record a deletion in the manifest
 */
export default {
  async fetch(request, env, ctx) {
    const apiKey = request.headers.get("X-Api-Key");
    if (apiKey !== env.API_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/push/intent") {
      return handlePushIntent(request, env);
    }

    if (request.method === "POST" && url.pathname === "/push/confirm") {
      return handlePushConfirm(request, env);
    }

    if (request.method === "POST" && url.pathname === "/push/delete") {
      return handlePushDelete(request, env);
    }

    return new Response("Not found", { status: 404 });
  }
};

/**
 * Records a set of deleted files in the manifest.
 *
 * Expects a JSON body: { pusher: string, files: [{ key: string, byteDelta: number }] }
 * Appends a single NDJSON entry to _manifest.jsonl with operation "delete".
 *
 * @param {Request} request
 * @param {object} env - Worker environment bindings (BUCKET)
 * @returns {Response} JSON { ok: true, deleted: number }
 */
async function handlePushDelete(request, env) {
  const { pusher, files } = await request.json();
  // files: [{ key: "data/Alabama/cleaned/committees.csv.gz", byteDelta: -153623 }, ...]

  const entry = {
    timestamp: new Date().toISOString(),
    pusher: pusher ?? "unknown",
    operation: "delete",
    files: files.map(f => ({ key: f.key, action: "deleted", byteDelta: f.byteDelta })),
  };

  const existing = await env.BUCKET.get(MANIFEST_KEY);
  const previous = existing ? await existing.text() : "";
  const updated = previous + JSON.stringify(entry) + "\n";
  await env.BUCKET.put(MANIFEST_KEY, updated, {
    httpMetadata: { contentType: "application/x-ndjson" },
  });

  return Response.json({ ok: true, deleted: files.length });
}



/**
 * Handles the first phase of a push: generates pre-signed S3 PutObject URLs
 * for each file the client wants to upload.
 *
 * For each file, checks whether it already exists in the bucket to determine
 * whether it is a new addition or a modification, and computes the byte delta.
 * Signed URLs expire after 15 minutes.
 *
 * Expects a JSON body: { files: [{ key: string, size: number, hash: string }] }
 *
 * @param {Request} request
 * @param {object} env - Worker environment bindings (BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
 * @returns {Response} JSON { files: [{ key, action, byteDelta, uploadUrl }] }
 */
async function handlePushIntent(request, env) {
  const { files } = await request.json();

  const s3 = makeS3Client(env);

  const results = await Promise.all(
    files.map(async ({ key, size, hash }) => {
      const existing = await env.BUCKET.head(key);

      const previousSize = existing?.size ?? null;
      const byteDelta = previousSize === null ? size : size - previousSize;
      const action = previousSize === null ? "added" : "modified";

      const uploadUrl = await getSignedUrl(
        s3,
        new PutObjectCommand({ Bucket: "state-campaign-finance", Key: key }),
        { expiresIn: 900 }
      );

      return { key, action, byteDelta, uploadUrl };
    })
  );

  return Response.json({ files: results });
}

/**
 * Handles the second phase of a push: records the completed upload in the manifest.
 *
 * Called by the client after it has finished uploading files to R2 via the
 * signed URLs from /push/intent. Appends a single NDJSON entry to
 * _manifest.jsonl with operation "push" and the file metadata provided by
 * the client.
 *
 * Expects a JSON body: { pusher: string, files: [{ key, action, byteDelta, ... }] }
 *
 * @param {Request} request
 * @param {object} env - Worker environment bindings (BUCKET)
 * @returns {Response} JSON { ok: true }
 */
async function handlePushConfirm(request, env) {
  const { pusher, files } = await request.json();

  const entry = {
    timestamp: new Date().toISOString(),
    pusher: pusher ?? "unknown",
    operation: "push",
    files,
  };

  // Read existing manifest, append new entry, write back
  const existing = await env.BUCKET.get(MANIFEST_KEY);
  const previous = existing ? await existing.text() : "";
  const updated = previous + JSON.stringify(entry) + "\n";
  await env.BUCKET.put(MANIFEST_KEY, updated, {
    httpMetadata: { contentType: "application/x-ndjson" },
  });

  return Response.json({ ok: true });
}

/**
 * Creates an S3Client configured for Cloudflare R2.
 *
 * @param {object} env - Worker environment bindings (R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)
 * @returns {S3Client}
 */
function makeS3Client(env) {
  return new S3Client({
    region: "auto",
    endpoint: `https://a4e15d50cae673b923618af708917cfa.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: env.R2_ACCESS_KEY_ID,
      secretAccessKey: env.R2_SECRET_ACCESS_KEY,
    },
  });
}
