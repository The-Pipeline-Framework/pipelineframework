#!/usr/bin/env node
import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {join, resolve} from 'node:path';
import {spawn} from 'node:child_process';
import {parseArgs} from 'node:util';
import {readJson, validateComponentsConfig} from './lib/contracts.mjs';
import {candidateBuildArguments, candidateFromOutput, expectedCandidateVersion, orderedMavenTargets} from './lib/compatibility-bootstrap.mjs';

const {values} = parseArgs({
  options: {
    components: {type: 'string'},
    baselineResolvedSet: {type: 'string'},
    targets: {type: 'string'},
    sources: {type: 'string'},
    mavenRepository: {type: 'string'},
    manifests: {type: 'string'},
    output: {type: 'string'},
    setId: {type: 'string'},
    coordinationRepository: {type: 'string'},
    coordinationSha: {type: 'string'},
    runId: {type: 'string'},
    runAttempt: {type: 'string'},
    java21Home: {type: 'string'},
    java25Home: {type: 'string'},
    runnerTemp: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baselineResolvedSet', 'targets', 'sources', 'mavenRepository', 'manifests', 'output', 'setId', 'coordinationRepository', 'coordinationSha', 'runId', 'runAttempt', 'java21Home', 'java25Home', 'runnerTemp']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const config = validateComponentsConfig(await readJson(values.components));
const baseline = await readJson(values.baselineResolvedSet);
const targetDocument = await readJson(values.targets);
if (targetDocument.schemaVersion !== 1 || !Array.isArray(targetDocument.targets) || targetDocument.targets.length < 2) {
  throw new Error('compatibility targets must contain at least two pull requests');
}
const targets = new Map();
for (const target of targetDocument.targets) {
  const component = config.components[target.component];
  if (component === undefined || component.repository !== target.repository) throw new Error(`invalid target component ${target.component}`);
  if (!/^[0-9a-f]{40}$/.test(target.sourceSha)) throw new Error(`invalid target SHA for ${target.component}`);
  if (!/^[0-9a-f]{40}$/.test(target.baseSha)) throw new Error(`invalid target base SHA for ${target.component}`);
  if (!/^[0-9a-f]{40}$/.test(target.testedSha)) throw new Error(`invalid tested SHA for ${target.component}`);
  if (!Number.isSafeInteger(target.pullRequestNumber) || target.pullRequestNumber < 1) throw new Error(`invalid PR number for ${target.component}`);
  if (targets.has(target.component)) throw new Error(`duplicate target component ${target.component}`);
  targets.set(target.component, target);
}

await mkdir(values.sources, {recursive: true});
await mkdir(values.mavenRepository, {recursive: true});
await mkdir(values.manifests, {recursive: true});
for (const target of targets.values()) await checkout(target);

const resolvedSet = structuredClone(baseline);
resolvedSet.candidates = [];
for (const target of orderedMavenTargets(config, targets)) {
  const component = config.components[target.component];
  const versionArguments = [];
  for (const [dependency, property] of Object.entries(component.consumerVersionProperties)) {
    const pin = resolvedSet.components[dependency];
    if (pin === undefined) throw new Error(`${target.component} references unresolved Maven component ${dependency}`);
    versionArguments.push(`-D${property}=${pin.mavenVersion}`);
  }
  versionArguments.sort();
  const source = join(values.sources, target.component);
  const javaHome = component.buildJavaVersion === 25 ? values.java25Home : values.java21Home;
  const candidateOutput = join(values.runnerTemp, `${target.component}-candidate-output.txt`);
  await mkdir(join(values.runnerTemp, target.component), {recursive: true});
  await run('bash', ['scripts/prepare-candidate.sh', 'pull_request', String(target.pullRequestNumber), target.sourceSha], {
    cwd: source,
    env: {...process.env, JAVA_HOME: javaHome, GITHUB_OUTPUT: candidateOutput, RUNNER_TEMP: join(values.runnerTemp, target.component)}
  });
  const candidateVersion = candidateFromOutput(await readFile(candidateOutput, 'utf8'));
  const expectedVersion = expectedCandidateVersion(resolvedSet, target);
  if (candidateVersion !== expectedVersion) {
    throw new Error(`${target.component} prepared ${candidateVersion}, expected ${expectedVersion}`);
  }
  await run(join(source, 'mvnw'), candidateBuildArguments(resolve(values.mavenRepository), versionArguments), {
    cwd: source,
    env: {...process.env, JAVA_HOME: javaHome}
  });
  const manifest = await writeManifest(target, candidateVersion, versionArguments);
  resolvedSet.components[target.component] = {
    repository: target.repository,
    sha: target.sourceSha,
    mavenVersion: candidateVersion,
    manifestDigest: manifest.digest
  };
  resolvedSet.candidates.push(candidateRecord(target, candidateVersion, manifest.digest));
}

for (const target of targets.values()) {
  if (config.components[target.component].kind !== 'source') continue;
  const candidateVersion = expectedCandidateVersion(resolvedSet, target);
  const manifest = await writeManifest(target, candidateVersion, []);
  resolvedSet.testHarnesses[target.component] = {repository: target.repository, sha: target.sourceSha};
  resolvedSet.candidates.push(candidateRecord(target, candidateVersion, manifest.digest));
}
resolvedSet.candidates.sort((left, right) => left.component.localeCompare(right.component));
await writeFile(values.output, `${JSON.stringify(resolvedSet, null, 2)}\n`);

function candidateRecord(target, candidateVersion, manifestDigest) {
  return {
    component: target.component,
    repository: target.repository,
    sha: target.sourceSha,
    candidateVersion,
    manifestDigest,
    suiteHints: []
  };
}

async function checkout(target) {
  const destination = join(values.sources, target.component);
  await mkdir(destination, {recursive: true});
  await run('git', ['init', '--quiet', destination]);
  await run('git', ['-C', destination, 'remote', 'add', 'origin', `https://github.com/${target.repository}.git`]);
  await run('git', ['-C', destination, '-c', 'protocol.version=2', 'fetch', '--quiet', '--depth=1', 'origin', target.testedSha]);
  await run('git', ['-C', destination, 'checkout', '--quiet', '--detach', 'FETCH_HEAD']);
  const actual = (await capture('git', ['-C', destination, 'rev-parse', 'HEAD'])).trim();
  if (actual !== target.testedSha) throw new Error(`checkout for ${target.component} resolved ${actual}`);
}

async function writeManifest(target, candidateVersion, versionArguments) {
  const component = config.components[target.component];
  const artifacts = [];
  for (const coordinate of component.allowedCoordinates ?? []) {
    const [groupId, artifactId, packaging] = coordinate.split(':');
    const directory = join(values.mavenRepository, ...groupId.split('.'), artifactId, candidateVersion);
    const primaryExtension = packaging === 'pom' ? 'pom' : 'jar';
    const names = [...new Set([`${artifactId}-${candidateVersion}.${primaryExtension}`, `${artifactId}-${candidateVersion}.pom`])];
    const files = [];
    for (const name of names) {
      const path = join(directory, name);
      const bytes = await readFile(path);
      files.push({name, sha256: createHash('sha256').update(bytes).digest('hex')});
    }
    artifacts.push({groupId, artifactId, version: candidateVersion, packaging, files});
  }
  const manifest = {
    schemaVersion: 1,
    setId: values.setId,
    repository: target.repository,
    component: target.component,
    sourceSha: target.sourceSha,
    baseSha: target.baseSha,
    testedSha: target.testedSha,
    pullRequestNumber: target.pullRequestNumber,
    candidateVersion,
    dependencyOverrides: versionArguments.map((argument) => argument.slice(2)).sort(),
    provenance: {
      repository: values.coordinationRepository,
      sha: values.coordinationSha,
      runId: Number(values.runId),
      runAttempt: Number(values.runAttempt),
      workflowPath: '.github/workflows/system-test-compatibility-set.yml'
    },
    mavenArtifacts: artifacts
  };
  const bytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
  const digest = `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
  const directory = join(values.manifests, target.component);
  await mkdir(directory, {recursive: true});
  await writeFile(join(directory, 'compatibility-candidate-manifest.json'), bytes);
  return {digest};
}

function run(command, arguments_, options = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, arguments_, {...options, stdio: 'inherit', shell: false});
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`${command} failed with ${signal === null ? `exit code ${code}` : `signal ${signal}`}`));
    });
  });
}

function capture(command, arguments_) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, arguments_, {stdio: ['ignore', 'pipe', 'inherit'], shell: false});
    let output = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) resolvePromise(output);
      else reject(new Error(`${command} failed with ${signal === null ? `exit code ${code}` : `signal ${signal}`}`));
    });
  });
}
