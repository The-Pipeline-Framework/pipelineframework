#!/usr/bin/env node

const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { chromium } = require("playwright");

function parseArgs(argv) {
  const args = {};
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === "--root") {
      args.root = next;
      index += 1;
    } else if (token === "--data") {
      args.data = next;
      index += 1;
    } else if (token === "--frames-dir") {
      args.framesDir = next;
      index += 1;
    } else if (token === "--poster") {
      args.poster = next;
      index += 1;
    }
  }
  if (!args.root || !args.data || !args.framesDir || !args.poster) {
    throw new Error("Usage: capture_frames.cjs --root <repo-root> --data <cinematic.json> --frames-dir <dir> --poster <jpg>");
  }
  return args;
}

function mimeType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js") || filePath.endsWith(".cjs") || filePath.endsWith(".mjs")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".png")) return "image/png";
  if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) return "image/jpeg";
  return "application/octet-stream";
}

function startServer(rootDir) {
  return new Promise((resolve) => {
    const resolvedRootDir = path.resolve(rootDir);
    const server = http.createServer((request, response) => {
      const requestUrl = new URL(request.url, "http://127.0.0.1");
      const decodedPath = decodeURIComponent(requestUrl.pathname);
      const relativeRequestPath = path.normalize(decodedPath).replace(/^([/\\])+/, "").replace(/^[A-Za-z]:[/\\]?/, "");
      const targetPath = path.resolve(resolvedRootDir, relativeRequestPath);
      const relativeTargetPath = path.relative(resolvedRootDir, targetPath);
      if (relativeTargetPath.startsWith("..") || path.isAbsolute(relativeTargetPath)) {
        response.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
        response.end("Forbidden");
        return;
      }
      fs.readFile(targetPath, (error, content) => {
        if (error) {
          response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
          response.end("Not found");
          return;
        }
        response.writeHead(200, { "Content-Type": mimeType(targetPath), "Cache-Control": "no-store" });
        response.end(content);
      });
    });
    server.listen(0, "127.0.0.1", () => resolve(server));
  });
}

async function main() {
  const args = parseArgs(process.argv);
  let server;
  let browser;
  try {
    server = await startServer(args.root);
    const address = server.address();
    const relativeDataPath = `/${path.relative(args.root, args.data).split(path.sep).join("/")}`;
    const sceneUrl = `http://127.0.0.1:${address.port}/tools/homepage-replay-video/render_scene.html?capture=1&data=${encodeURIComponent(relativeDataPath)}`;
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({ viewport: { width: 1600, height: 900 }, deviceScaleFactor: 1 });
    await page.goto(sceneUrl, { waitUntil: "networkidle" });
    await page.waitForFunction(() => window.homepageReplayCinematic?.ready === true, null, { timeout: 30000 });

    const { clipDurationSeconds, fps } = await page.evaluate(() => ({
      clipDurationSeconds: window.homepageReplayCinematic?.cinematicData?.clipDurationSeconds,
      fps: window.homepageReplayCinematic?.cinematicData?.fps
    }));
    if (!clipDurationSeconds || !fps) {
      throw new Error("Missing cinematic timing metadata for frame capture.");
    }
    const totalFrames = Math.ceil(clipDurationSeconds * fps);
    const posterFrame = Math.round((totalFrames - 1) * 0.58);
    const captureRoot = page.locator("#captureRoot");
    fs.mkdirSync(args.framesDir, { recursive: true });
    fs.mkdirSync(path.dirname(args.poster), { recursive: true });

    for (let frame = 0; frame < totalFrames; frame += 1) {
      const progress = totalFrames <= 1 ? 0 : frame / (totalFrames - 1);
      await page.evaluate(async (value) => {
        window.homepageReplayCinematic.renderFrame(value);
        await new Promise((resolve) => requestAnimationFrame(() => resolve()));
      }, progress);
      const framePath = path.join(args.framesDir, `frame_${String(frame + 1).padStart(4, "0")}.png`);
      await captureRoot.screenshot({ path: framePath, type: "png" });
      if (frame === posterFrame) {
        await captureRoot.screenshot({ path: args.poster, type: "jpeg", quality: 90 });
      }
    }
  } finally {
    if (browser) {
      await browser.close();
    }
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
