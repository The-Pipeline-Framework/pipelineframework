#!/usr/bin/env node
import {readFile, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {parseArgs} from 'node:util';
import {pinDependencyProperties} from './lib/compatibility-bootstrap.mjs';

const {values} = parseArgs({options: {
  mode: {type: 'string'}, source: {type: 'string'}, sourceSha: {type: 'string'},
  resolvedSet: {type: 'string'}, mavenRepository: {type: 'string'}
}, strict: true});
for (const key of ['mode', 'source', 'sourceSha', 'resolvedSet', 'mavenRepository']) {
  if (!values[key]) throw new Error(`--${key} is required`);
}
if (!['build', 'test'].includes(values.mode)) throw new Error('mode must be build or test');
if (!/^[a-f0-9]{40}$/.test(values.sourceSha)) throw new Error('reference source must be a full SHA');
const set = JSON.parse(await readFile(values.resolvedSet, 'utf8'));
if (set.testHarnesses?.references?.sha !== values.sourceSha ||
    set.testHarnesses?.references?.repository !== 'The-Pipeline-Framework/pipelineframework-reference-implementations') {
  throw new Error('cloud reference source does not match the resolved full-train set');
}
const config = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8'));
const pins = {};
for (const [component, property] of Object.entries(config.components.references.consumerVersionProperties)) {
  const version = (component === 'coordinationBom' ? set.coordinationBom : set.components?.[component])?.mavenVersion;
  if (typeof version !== 'string' || !/^[0-9]+\.[0-9]+\.[0-9]+(?:-[a-zA-Z0-9.]+)?$/.test(version) || version.endsWith('-SNAPSHOT')) {
    throw new Error(`cloud suite requires an immutable ${component} Maven version`);
  }
  pins[property] = version;
}
if (values.mode === 'test' && !/^https:\/\/[a-z0-9]+\.lambda-url\.[a-z0-9-]+\.on\.aws\/?$/.test(process.env.AWS_LAMBDA_ORCHESTRATOR_URL ?? '')) {
  throw new Error('cloud test requires the deployed AWS Lambda Function URL; skipping is forbidden');
}
const pom = join(values.source, 'pom.xml');
await writeFile(pom, pinDependencyProperties(await readFile(pom, 'utf8'), pins));
const args = ['-B', '--no-transfer-progress', `-Dmaven.repo.local=${values.mavenRepository}`,
  ...Object.entries(pins).map(([property, version]) => `-D${property}=${version}`)];
const command = values.mode === 'build' ? 'bash' : './mvnw';
const commandArgs = values.mode === 'build'
  ? ['./search/build-lambda-modular.sh', '-DskipTests', '-Dquarkus.container-image.build=false']
  : [...args, '-f', 'search/pom.xml', '-pl', 'orchestrator-svc', '-am',
    '-DskipUnitTests=true', '-Dfailsafe.failIfNoSpecifiedTests=false',
    '-Dquarkus.container-image.build=false', '-Dit.test=AwsLambdaModularEndToEndIT', 'verify'];
const child = spawn(command, commandArgs, {cwd: values.source, stdio: 'inherit', shell: false,
  env: {...process.env, MAVEN_ARGS: args.join(' ')}});
process.exitCode = await new Promise((resolve, reject) => {
  child.once('error', reject);
  child.once('exit', (code, signal) => signal ? reject(new Error(`cloud suite terminated by ${signal}`)) : resolve(code ?? 1));
});
if (values.mode === 'test' && process.exitCode === 0) {
  const report = await readFile(join(values.source, 'search/orchestrator-svc/target/failsafe-reports',
    'TEST-org.pipelineframework.search.orchestrator.service.AwsLambdaModularEndToEndIT.xml'), 'utf8');
  const suite = report.match(/<testsuite\b[^>]*>/)?.[0] ?? '';
  const count = (attribute) => Number(suite.match(new RegExp(`\\b${attribute}="([0-9]+)"`))?.[1] ?? NaN);
  if (!(count('tests') >= 3 && count('skipped') === 0 && count('failures') === 0 && count('errors') === 0)) {
    throw new Error('AWS modular E2E evidence must contain at least three passing, unskipped tests');
  }
}
