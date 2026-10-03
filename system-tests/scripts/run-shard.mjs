#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import { delimiter, join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';

const {values} = parseArgs({
  options: {
    shard: {type: 'string'},
    coordination: {type: 'string'},
    owners: {type: 'string'},
    resolvedSet: {type: 'string'},
    mavenRepository: {type: 'string'}
  },
  strict: true
});
for (const name of ['shard', 'coordination', 'owners', 'resolvedSet', 'mavenRepository']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const shard = JSON.parse(await readFile(values.shard, 'utf8'));
if (!Array.isArray(shard.suites) || shard.suites.length === 0) throw new Error('shard must contain at least one suite');
const runner = new URL('run-suite.mjs', import.meta.url).pathname;
const components = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8')).components;
const failures = [];
for (const suite of shard.suites) {
  const suiteRoot = suite.owner === 'coordination'
    ? resolve(values.coordination)
    : resolve(values.owners, suite.owner);
  const manifest = suite.owner === 'coordination'
    ? join(suiteRoot, '.github', 'tpf-system-tests.json')
    : join(suiteRoot, suite.manifest);
  process.stdout.write(`::group::${suite.suite}\n`);
  try {
    const javaVersion = components[suite.owner]?.buildJavaVersion ?? 21;
    const javaHome = process.env[`TPF_JAVA_${javaVersion}_HOME`];
    if (!javaHome) throw new Error(`JDK ${javaVersion} is required for ${suite.owner}; TPF_JAVA_${javaVersion}_HOME is missing`);
    const environment = {
      ...process.env,
      JAVA_HOME: javaHome,
      PATH: `${join(javaHome, 'bin')}${delimiter}${process.env.PATH ?? ''}`
    };
    await run(process.execPath, [
      runner,
      '--manifest', manifest,
      '--entrypoint', suite.entrypoint,
      '--resolvedSet', resolve(values.resolvedSet),
      '--versionProperties', JSON.stringify(suite.versionProperties),
      '--cwd', suiteRoot,
      '--mavenRepository', resolve(values.mavenRepository)
    ], environment);
  } catch (error) {
    failures.push(suite.suite);
    process.stderr.write(`${error.message}\n`);
  } finally {
    process.stdout.write('::endgroup::\n');
  }
}
if (failures.length > 0) throw new Error(`product shard failed: ${failures.join(', ')}`);

function run(command, arguments_, environment) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, arguments_, {stdio: 'inherit', shell: false, env: environment});
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`${command} failed with ${signal === null ? `exit code ${code}` : `signal ${signal}`}`));
    });
  });
}
