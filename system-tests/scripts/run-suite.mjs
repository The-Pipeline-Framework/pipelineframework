#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { parseArgs } from 'node:util';
import { readJson } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    manifest: {type: 'string'},
    entrypoint: {type: 'string'},
    resolvedSet: {type: 'string'},
    versionProperties: {type: 'string'},
    cwd: {type: 'string'},
    mavenRepository: {type: 'string'}
  },
  strict: true
});
for (const name of ['manifest', 'entrypoint', 'resolvedSet', 'versionProperties', 'cwd', 'mavenRepository']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const manifest = await readJson(values.manifest);
if (manifest.schemaVersion !== 1 || manifest.suites === null || typeof manifest.suites !== 'object' || Array.isArray(manifest.suites)) {
  throw new Error('suite manifest is invalid');
}
const suite = manifest.suites[values.entrypoint];
if (suite === undefined) throw new Error(`suite manifest does not define entrypoint ${values.entrypoint}`);
if (!Array.isArray(suite.command) || suite.command.length === 0 || suite.command.some((part) => typeof part !== 'string' || part.length === 0)) {
  throw new Error('suite command must be a non-empty string array');
}
if (!Number.isSafeInteger(suite.timeoutMinutes) || suite.timeoutMinutes < 1 || suite.timeoutMinutes > 360) {
  throw new Error('suite timeoutMinutes must be between 1 and 360');
}
const resolvedSet = await readJson(values.resolvedSet);
const versionProperties = JSON.parse(values.versionProperties);
if (versionProperties === null || typeof versionProperties !== 'object' || Array.isArray(versionProperties)) {
  throw new Error('--versionProperties must be a JSON object');
}
const versionArguments = [];
for (const [component, property] of Object.entries(versionProperties)) {
  const pin = resolvedSet.components?.[component];
  if (pin === undefined) throw new Error(`suite references unresolved Maven component ${component}`);
  if (typeof property !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(property)) throw new Error(`suite has invalid Maven property for ${component}`);
  versionArguments.push(`-D${property}=${pin.mavenVersion}`);
}
versionArguments.sort();
const [command, ...arguments_] = suite.command;
const injectedMavenArguments = [...versionArguments, `-Dmaven.repo.local=${values.mavenRepository}`];
const mavenArguments = injectedMavenArguments.join(' ');
const directMavenCommand = /(?:^|[\\/])mvnw(?:\.cmd)?$/.test(command);
const commandArguments = directMavenCommand ? [...arguments_, ...injectedMavenArguments] : arguments_;
const child = spawn(command, commandArguments, {
  cwd: values.cwd,
  env: {
    ...process.env,
    MAVEN_ARGS: [process.env.MAVEN_ARGS, mavenArguments].filter(Boolean).join(' '),
    TPF_RESOLVED_SET: values.resolvedSet
  },
  stdio: 'inherit',
  shell: false
});
const timeout = setTimeout(() => {
  child.kill('SIGTERM');
  setTimeout(() => child.kill('SIGKILL'), 10_000).unref();
}, suite.timeoutMinutes * 60_000);
timeout.unref();
const exitCode = await new Promise((resolve, reject) => {
  child.once('error', reject);
  child.once('exit', (code, signal) => signal === null ? resolve(code ?? 1) : reject(new Error(`suite terminated by ${signal}`)));
});
clearTimeout(timeout);
process.exitCode = exitCode;
