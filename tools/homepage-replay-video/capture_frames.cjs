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
    const server = http.createServer((request, response) => {
      const requestUrl = new URL(request.url, "http://127.0.0.1");
      const requestedPath = path.normalize(decodeURIComponent(requestUrl.pathname)).replace(/^(\.\.[/\\])+/, "");
      const targetPath = path.join(rootDir, requestedPath);
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
  const server = await startServer(args.root);
  const address = server.address();
  const relativeDataPath = `/${path.relative(args.root, args.data).split(path.sep).join("/")}`;
  const sceneUrl = `http://127.0.0.1:${address.port}/tools/homepage-replay-video/render_scene.html?capture=1&data=${encodeURIComponent(relativeDataPath)}`;
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 }, deviceScaleFactor: 1 });
  await page.goto(sceneUrl, { waitUntil: "networkidle" });
  await page.waitForFunction(() => window.homepageReplayCinematic?.ready === true, null, { timeout: 30000 });

  const totalFrames = 12 * 24;
  const captureRoot = page.locator("#captureRoot");
  fs.mkdirSync(args.framesDir, { recursive: true });

  await page.evaluate(() => window.homepageReplayCinematic.renderFrame(0.58));
  await captureRoot.screenshot({ path: args.poster, type: "jpeg", quality: 90 });

  for (let frame = 0; frame < totalFrames; frame += 1) {
    const progress = totalFrames <= 1 ? 0 : frame / (totalFrames - 1);
    await page.evaluate((value) => window.homepageReplayCinematic.renderFrame(value), progress);
    const framePath = path.join(args.framesDir, `frame_${String(frame + 1).padStart(4, "0")}.png`);
    await captureRoot.screenshot({ path: framePath, type: "png" });
  }

  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
