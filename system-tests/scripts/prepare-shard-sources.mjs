#!/usr/bin/env node
import { mkdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';

const {values} = parseArgs({
  options: {
    shard: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['shard', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const shard = JSON.parse(await readFile(values.shard, 'utf8'));
if (!Array.isArray(shard.suites) || shard.suites.length === 0) throw new Error('shard must contain at least one suite');
const owners = new Map();
for (const suite of shard.suites) {
  if (suite.owner === 'coordination') continue;
  if (!/^[A-Za-z0-9._-]+$/.test(suite.owner)) throw new Error(`invalid suite owner ${suite.owner}`);
  if (!/^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/.test(suite.repository)) throw new Error(`invalid repository ${suite.repository}`);
  if (!/^[0-9a-f]{40}$/.test(suite.sha)) throw new Error(`invalid source SHA for ${suite.owner}`);
  const existing = owners.get(suite.owner);
  if (existing !== undefined && (existing.repository !== suite.repository || existing.sha !== suite.sha)) {
    throw new Error(`shard contains conflicting pins for ${suite.owner}`);
  }
  owners.set(suite.owner, {repository: suite.repository, sha: suite.sha});
}

await mkdir(values.output, {recursive: true});
for (const [owner, pin] of [...owners.entries()].sort(([left], [right]) => left.localeCompare(right))) {
  const destination = join(values.output, owner);
  await mkdir(destination, {recursive: true});
  await run('git', ['init', '--quiet', destination]);
  await run('git', ['-C', destination, 'remote', 'add', 'origin', `https://github.com/${pin.repository}.git`]);
  await run('git', ['-C', destination, '-c', 'protocol.version=2', 'fetch', '--quiet', '--depth=1', 'origin', pin.sha]);
  await run('git', ['-C', destination, 'checkout', '--quiet', '--detach', 'FETCH_HEAD']);
  const actual = (await capture('git', ['-C', destination, 'rev-parse', 'HEAD'])).trim();
  if (actual !== pin.sha) throw new Error(`checkout for ${owner} resolved ${actual}, expected ${pin.sha}`);
}

function run(command, arguments_) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, arguments_, {stdio: 'inherit', shell: false});
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} failed with ${signal === null ? `exit code ${code}` : `signal ${signal}`}`));
    });
  });
}

function capture(command, arguments_) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, arguments_, {stdio: ['ignore', 'pipe', 'inherit'], shell: false});
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolve(output);
      else reject(new Error(`${command} failed with ${signal === null ? `exit code ${code}` : `signal ${signal}`}`));
    });
  });
}
