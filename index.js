const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");
const { createClient } = require("@supabase/supabase-js");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const https = require("https");
const crypto = require("crypto");

process.env.PATH = `/opt/bin:${process.env.PATH}`;

const s3 = new S3Client({ region: "ap-south-1" });
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const BUCKET = process.env.S3_BUCKET || "cpacontentstream";
const TMP = "/tmp";

/**
 * ✅ FIX: webhookController sends the FULL webhook URL already:
 *   event.webhookUrl = "https://backend.render.com/api/videos/studio/webhook/video-ready"
 *
 * The old code was appending "/api/videos/studio/webhook/video-ready" again,
 * producing a doubled path that always 404s.
 * 
 * Now we POST directly to webhookUrl as-is.
 */
async function callWebhook(webhookUrl, webhookSecret, jobId, status, manifestUrl = null, error = null) {
  if (!webhookUrl) return;

  const payload = JSON.stringify({ jobId, status, manifestUrl, error });
  const signature = webhookSecret
    ? crypto.createHmac("sha256", webhookSecret).update(payload).digest("hex")
    : null;

  return new Promise((resolve) => {
    const url = new URL(webhookUrl); // ✅ use as-is — no extra path appended

    const options = {
      hostname: url.hostname,
      port:     url.port || 443,
      path:     url.pathname + url.search,  // preserve the full path
      method:   "POST",
      headers: {
        "Content-Type":   "application/json",
        "Content-Length": Buffer.byteLength(payload),
        ...(signature && { "x-cpa-signature": signature }),
      },
    };

    const req = https.request(options, (res) => {
      console.log(`[${jobId}] Webhook → ${res.statusCode}`);
      resolve();
    });

    req.on("error", (err) => {
      console.error(`[${jobId}] Webhook failed:`, err.message);
      resolve(); // don't throw — Lambda already did its job
    });

    req.write(payload);
    req.end();
  });
}

async function logStage(jobId, stage, status, message = null, durationMs = null) {
  try {
    await supabase.from("video_job_logs").insert({
      job_id: jobId, stage, status, message, duration_ms: durationMs,
    });
    console.log(`[${jobId}] ${stage} → ${status}${message ? `: ${message}` : ""}`);
  } catch (err) {
    console.error(`[${jobId}] Log failed:`, err.message);
  }
}

async function updateJobStatus(jobId, status, extra = {}) {
  await supabase.from("video_jobs").update({ status, ...extra }).eq("id", jobId);
}

function cleanup(...paths) {
  for (const p of paths) {
    try {
      if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
    } catch (_) {}
  }
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function uploadToS3(localPath, s3Key, contentType) {
  const body = fs.readFileSync(localPath);
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key:    s3Key,
    Body:   body,
    ContentType: contentType,
    CacheControl: contentType === "application/x-mpegURL"
      ? "no-cache"
      : "max-age=31536000, immutable",
  }));
}

async function transcodeToHLS(inputPath, outputDir) {
  fs.mkdirSync(outputDir, { recursive: true });
  const playlistPath = path.join(outputDir, "playlist.m3u8");
  await new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", [
      "-i", inputPath,
      "-c:v", "libx264", "-preset", "fast", "-crf", "23",
      "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
      "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
      "-hls_time", "4",
      "-hls_playlist_type", "vod",
      "-hls_segment_filename", path.join(outputDir, "segment%03d.ts"),
      "-hls_flags", "independent_segments",
      playlistPath,
    ]);
    ffmpeg.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with code ${code}`));
    });
    ffmpeg.on("error", reject);
  });
  return playlistPath;
}

async function uploadHLSToS3(outputDir, jobId) {
  const files = fs.readdirSync(outputDir);
  for (const file of files) {
    const s3Key = `hls/${jobId}/${file}`;
    const contentType = file.endsWith(".m3u8") ? "application/x-mpegURL" : "video/mp2t";
    await uploadToS3(path.join(outputDir, file), s3Key, contentType);
  }
  return `https://${BUCKET}.s3.ap-south-1.amazonaws.com/hls/${jobId}/playlist.m3u8`;
}

exports.handler = async (event) => {
  const s3Key = event.Records?.[0]?.s3?.object?.key
    ? decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "))
    : event.s3Key;

  if (!s3Key) {
    return { statusCode: 400, body: JSON.stringify({ error: "No s3Key" }) };
  }

  const bucketName   = event.Records?.[0]?.s3?.bucket?.name || event.bucket || BUCKET;
  const head         = await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: s3Key }));
  const jobId        = head.Metadata?.job_id || event.jobId;
  const webhookUrl   = event.webhookUrl   || process.env.WEBHOOK_URL;
  const webhookSecret = event.webhookSecret || process.env.WEBHOOK_SECRET;

  if (!jobId) {
    return { statusCode: 400, body: JSON.stringify({ error: "No job_id" }) };
  }

  const inputPath = path.join(TMP, `${jobId}_input.mp4`);
  const outputDir = path.join(TMP, `${jobId}_hls`);

  console.log(`[${jobId}] Lambda started — s3Key: ${s3Key}, webhookUrl: ${webhookUrl}`);

  try {
    const processStart = Date.now();
    await updateJobStatus(jobId, "PROCESSING");
    await logStage(jobId, "processing", "started", `S3 key: ${s3Key}`);

    const res = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: s3Key }));
    const buffer = await streamToBuffer(res.Body);
    fs.writeFileSync(inputPath, buffer);
    await logStage(jobId, "processing", "downloaded", `${(buffer.length / 1024 / 1024).toFixed(2)}MB`);

    const chunkStart = Date.now();
    await updateJobStatus(jobId, "CHUNKING");
    await logStage(jobId, "chunking", "started");
    await transcodeToHLS(inputPath, outputDir);
    await logStage(jobId, "chunking", "completed", null, Date.now() - chunkStart);

    const uploadStart = Date.now();
    await logStage(jobId, "uploading_chunks", "started");
    const manifestUrl = await uploadHLSToS3(outputDir, jobId);
    await logStage(jobId, "uploading_chunks", "completed", null, Date.now() - uploadStart);

    await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: s3Key }));
    console.log(`[${jobId}] Raw file deleted from S3`);

    await updateJobStatus(jobId, "READY", { manifest_url: manifestUrl });
    await logStage(jobId, "ready", "completed", manifestUrl, Date.now() - processStart);

    // ✅ webhookUrl is already the full URL — callWebhook no longer appends a path
    await callWebhook(webhookUrl, webhookSecret, jobId, "ready", manifestUrl);

    cleanup(inputPath, outputDir);

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, jobId, manifestUrl }),
    };

  } catch (err) {
    console.error(`[${jobId}] Failed:`, err.message);
    await updateJobStatus(jobId, "FAILED", { error: err.message });
    await logStage(jobId, "processing", "failed", err.message);
    await callWebhook(webhookUrl, webhookSecret, jobId, "failed", null, err.message);
    cleanup(inputPath, outputDir);
    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, jobId, error: err.message }),
    };
  }
};
      
