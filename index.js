/**
 * cpa-video-processor — Lambda
 *
 * Stateless media processor. Does exactly 4 things:
 *   1. Download original MP4 from S3 (uploads/original/{job_id}.mp4)
 *   2. Convert MP4 → HLS using FFmpeg
 *   3. Upload HLS files to S3 (videos/YYYY/MM/{reel_id}/)
 *   4. POST callback to backend with the CloudFront stream URL
 *
 * No Supabase access. No state. Backend is the single source of truth.
 */

const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");
const { spawn } = require("child_process");
const fs    = require("fs");
const path  = require("path");
const https = require("https");
const crypto = require("crypto");

process.env.PATH = `/opt/bin:${process.env.PATH}`;

const s3  = new S3Client({ region: process.env.AWS_REGION || "ap-south-1" });
const TMP = "/tmp";

// ── Helpers ─────────────────────────────────────────────────────────────

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function downloadFromS3(bucket, key, localPath) {
  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const buffer = await streamToBuffer(res.Body);
  fs.writeFileSync(localPath, buffer);
  return buffer.length;
}

async function transcodeToHLS(inputPath, outputDir) {
  fs.mkdirSync(outputDir, { recursive: true });
  const indexPath = path.join(outputDir, "index.m3u8");

  await new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", [
      "-i", inputPath,
      "-c:v", "libx264", "-preset", "fast", "-crf", "23",
      "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2",
      "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
      "-hls_time", "4",
      "-hls_playlist_type", "vod",
      "-hls_segment_filename", path.join(outputDir, "seg%03d.ts"),
      "-hls_flags", "independent_segments",
      indexPath,
    ]);

    let stderr = "";
    ffmpeg.stderr.on("data", d => { stderr += d.toString(); });

    ffmpeg.on("close", code => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with code ${code}: ${stderr.slice(-300)}`));
    });
    ffmpeg.on("error", reject);
  });

  return indexPath;
}

/**
 * Get video duration in seconds using ffprobe.
 */
async function getDuration(inputPath) {
  return new Promise((resolve) => {
    let out = "";
    const ffprobe = spawn("ffprobe", [
      "-v", "error",
      "-show_entries", "format=duration",
      "-of", "default=noprint_wrappers=1:nokey=1",
      inputPath,
    ]);
    ffprobe.stdout.on("data", d => { out += d.toString(); });
    ffprobe.on("close", () => resolve(parseFloat(out.trim()) || 0));
    ffprobe.on("error", () => resolve(0));
  });
}

/**
 * Writes master.m3u8 — a single-quality master playlist pointing at index.m3u8.
 * Frontend always loads master.m3u8 so adding more qualities later requires
 * no frontend changes.
 */
function writeMasterPlaylist(outputDir, bandwidth = 2500000, resolution = "1080x1920") {
  const masterPath = path.join(outputDir, "master.m3u8");
  const content = [
    "#EXTM3U",
    `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolution}`,
    "index.m3u8",
    "",
  ].join("\n");
  fs.writeFileSync(masterPath, content);
  return masterPath;
}

async function uploadDirToS3(localDir, bucket, s3Prefix) {
  const files = fs.readdirSync(localDir);
  for (const file of files) {
    const localPath = path.join(localDir, file);
    const key = `${s3Prefix}${file}`;
    const contentType = file.endsWith(".m3u8")
      ? "application/x-mpegURL"
      : "video/mp2t";

    await s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fs.readFileSync(localPath),
      ContentType: contentType,
      CacheControl: file.endsWith(".m3u8")
        ? "no-cache"
        : "max-age=31536000, immutable",
    }));
  }
}

function cleanup(...paths) {
  for (const p of paths) {
    try {
      if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
    } catch (_) {}
  }
}

/**
 * POSTs the result to the backend's callback_url with a Bearer token.
 * Does not throw — logs and returns on failure since Lambda has already
 * done its job; the backend can retry/poll if needed.
 */
async function postCallback(callbackUrl, callbackToken, payload) {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload);
    const url = new URL(callbackUrl);

    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        ...(callbackToken ? { Authorization: `Bearer ${callbackToken}` } : {}),
      },
    };

    const req = https.request(options, res => {
      console.log(`[${payload.job_id}] Callback → ${res.statusCode}`);
      resolve();
    });
    req.on("error", err => {
      console.error(`[${payload.job_id}] Callback failed:`, err.message);
      resolve();
    });
    req.write(body);
    req.end();
  });
}

// ── Handler ─────────────────────────────────────────────────────────────

exports.handler = async (event) => {
  const {
    job_id,
    reel_id,
    bucket,
    input_key,
    callback_url,
    callback_token,
    cloudfront_domain,
  } = event;

  if (!job_id || !reel_id || !bucket || !input_key || !callback_url) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "job_id, reel_id, bucket, input_key, callback_url required" }),
    };
  }

  const cdnDomain = cloudfront_domain || process.env.CLOUDFRONT_DOMAIN || "cdn.codeplusacademy.in";

  const inputPath = path.join(TMP, `${job_id}_input.mp4`);
  const outputDir = path.join(TMP, `${job_id}_hls`);

  console.log(`[${job_id}] Started — input: s3://${bucket}/${input_key}`);

  try {
    // ── Step 1: Download original from S3 ─────────────────────────────
    const bytes = await downloadFromS3(bucket, input_key, inputPath);
    console.log(`[${job_id}] Downloaded ${(bytes / 1024 / 1024).toFixed(2)}MB`);

    // ── Step 2: FFmpeg → HLS ────────────────────────────────────────────
    await transcodeToHLS(inputPath, outputDir);
    writeMasterPlaylist(outputDir);
    const duration = await getDuration(inputPath);
    console.log(`[${job_id}] Transcoded — duration: ${duration}s`);

    // ── Step 3: Upload HLS to videos/YYYY/MM/{reel_id}/ ────────────────
    const now = new Date();
    const yyyy = now.getUTCFullYear();
    const mm   = String(now.getUTCMonth() + 1).padStart(2, "0");
    const s3Prefix = `videos/${yyyy}/${mm}/${reel_id}/`;

    await uploadDirToS3(outputDir, bucket, s3Prefix);
    console.log(`[${job_id}] Uploaded HLS to s3://${bucket}/${s3Prefix}`);

    // Delete the original raw upload — no longer needed
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: input_key }));

    // ── Step 4: Notify backend ──────────────────────────────────────────
    const masterPlaylistS3  = `s3://${bucket}/${s3Prefix}master.m3u8`;
    const masterPlaylistUrl = `https://${cdnDomain}/${s3Prefix}master.m3u8`;

    await postCallback(callback_url, callback_token, {
      job_id,
      reel_id,
      status: "completed",
      bucket,
      s3_prefix: s3Prefix,
      master_playlist_s3: masterPlaylistS3,
      master_playlist_url: masterPlaylistUrl,
      stream_url: masterPlaylistUrl,
      duration,
      processed_at: new Date().toISOString(),
    });

    cleanup(inputPath, outputDir);

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, job_id, reel_id, stream_url: masterPlaylistUrl }),
    };

  } catch (err) {
    console.error(`[${job_id}] Failed:`, err.message);

    await postCallback(callback_url, callback_token, {
      job_id,
      reel_id,
      status: "failed",
      error: err.message,
      processed_at: new Date().toISOString(),
    });

    cleanup(inputPath, outputDir);

    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, job_id, reel_id, error: err.message }),
    };
  }
};
