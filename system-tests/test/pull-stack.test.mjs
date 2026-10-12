import assert from 'node:assert/strict';
import {mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';
import {pullStack, stackRevision, assertCurrentStacks} from '../scripts/lib/pull-stack.mjs';
import {discoverCompatibilitySet} from '../scripts/lib/compatibility-discovery.mjs';

const repository = 'The-Pipeline-Framework/pipelineframework-runtime';
const config = {components: {runtime: {repository}}};
const layer = (number, headRef, baseRef, sha) => ({number, state: 'open',
  base: {ref: baseRef, repo: {full_name: repository}},
  head: {ref: headRef, sha, repo: {owner: {login: 'The-Pipeline-Framework'}}}});
const pulls = () => [layer(47, 'lower', 'main', 'a'.repeat(40)), layer(48, 'upper', 'lower', 'b'.repeat(40))];
function discovery(number = 47) {
  return discoverCompatibilitySet(config, pulls(), {sourceRepository: repository,
    sourcePullRequest: number, sourceSha: number === 47 ? 'a'.repeat(40) : 'b'.repeat(40)});
}

test('both stack layers resolve to one stable top candidate and revision', () => {
  assert.deepEqual(discovery(47), discovery(48));
  assert.deepEqual(discovery().pullRequests, [`https://github.com/${repository}/pull/48`]);
  assert.deepEqual(discovery().stacks[0].layers.map(layer => layer.number), [47, 48]);
});
test('head, parent, membership and base changes invalidate the exact stack', () => {
  const snapshots = [pullStack(pulls(), repository, 47)];
  assertCurrentStacks(snapshots, pulls());
  for (const change of ['lower', 'upper', 'base', 'added', 'removed']) {
    const modified = pulls();
    if (change === 'lower') modified[0].head.sha = 'c'.repeat(40);
    if (change === 'upper') modified[1].head.sha = 'c'.repeat(40);
    if (change === 'base') modified[0].base.ref = 'other-main';
    if (change === 'added') modified.push(layer(49, 'highest', 'upper', 'c'.repeat(40)));
    if (change === 'removed') modified.pop();
    assert.throws(() => assertCurrentStacks(snapshots, modified), /obsolete/);
  }
});
test('ambiguous siblings and cycles never inherit green stack coverage', () => {
  assert.throws(() => pullStack([...pulls(), layer(49, 'sibling', 'lower', 'c'.repeat(40))], repository, 47), /branched/);
  const cyclic = pulls(); cyclic[0].base.ref = 'upper';
  assert.throws(() => pullStack(cyclic, repository, 47), /cyclic/);
});

test('one repository is accepted only with a validated multi-layer snapshot', async () => {
  const root = await mkdtemp(join(tmpdir(), 'tpf-stack-parser-'));
  const input = join(root, 'pulls.txt'), output = join(root, 'request.json');
  await writeFile(input, discovery().pullRequests.join('\n'));
  const args = [new URL('../scripts/parse-compatibility-prs.mjs', import.meta.url).pathname,
    '--components', new URL('../components.yml', import.meta.url).pathname,
    '--input', input, '--output', output, '--setId', discovery().setId];
  assert.notEqual(spawnSync(process.execPath, args, {encoding: 'utf8'}).status, 0);
  const valid = spawnSync(process.execPath, [...args, '--stacks', JSON.stringify(discovery().stacks),
    '--revision', discovery().revision], {encoding: 'utf8'});
  assert.equal(valid.status, 0, valid.stderr);
  assert.deepEqual(JSON.parse(await readFile(output, 'utf8')).stackSnapshot, discovery().stacks);
  const invalid = spawnSync(process.execPath, [...args, '--stacks', JSON.stringify(discovery().stacks),
    '--revision', 'f'.repeat(64)], {encoding: 'utf8'});
  assert.notEqual(invalid.status, 0);
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'tpf-stack-'));
  await writeFile(join(root, 'pulls.json'), JSON.stringify(pulls()));
  const plan = discovery();
  await writeFile(join(root, 'plan.json'), JSON.stringify(plan));
  await writeFile(join(root, 'gh'), `#!/usr/bin/env node
const fs = require('node:fs'); const path = require('node:path');
const root = process.env.FAKE_ROOT, route = process.argv[3], args = process.argv.slice(4);
const method = args.includes('--method') ? args[args.indexOf('--method') + 1] : 'GET';
const ref = 'refs/tags/tpf-system-test-stack/' + process.env.SET_ID + '/' + process.env.REVISION;
const claim = path.join(root, process.env.REVISION);
if (route.includes('/pulls?')) console.log(JSON.stringify([JSON.parse(fs.readFileSync(path.join(root,'pulls.json')))]));
else if (route.includes('/compare/')) console.log(JSON.stringify({status:process.env.STALE ? 'diverged':'ahead',merge_base_commit:{sha:process.env.STALE ? 'c'.repeat(40):'a'.repeat(40)}}));
else if (route.includes('/commits/')) console.log(JSON.stringify({sha:'d'.repeat(40)}));
else if (route.endsWith('/git/refs')) { try { fs.closeSync(fs.openSync(claim,'wx')); console.log('{}'); } catch { process.exit(1); } }
else if (route.includes('/git/ref/')) { if(!fs.existsSync(claim)) process.exit(1); console.log(JSON.stringify({ref})); }
else if (route.includes('/git/refs/') && method==='DELETE') { fs.unlinkSync(claim); }
else if (route.includes('/statuses/')) { fs.appendFileSync(path.join(root,'statuses'), args.join(' ')+'\\n'); console.log('{}'); }
else if (route.includes('/dispatches')) { if(process.env.FAIL_DISPATCH) process.exit(1); fs.readFileSync(0); fs.appendFileSync(path.join(root,'dispatches'),'dispatch\\n'); }
else throw new Error('unexpected API '+route);
`, {mode: 0o755});
  return {root, plan};
}
function run(f, mode, extra = {}) {
  return spawnSync(process.execPath, [new URL('../scripts/coordinate-stack.mjs', import.meta.url).pathname,
    '--mode', mode, '--plan', join(f.root, 'plan.json'), '--targets', join(f.root, 'targets.json'),
    '--coordinator', 'The-Pipeline-Framework/pipelineframework', '--coordinatorSha', 'e'.repeat(40)],
  {encoding: 'utf8', env: {...process.env, PATH: `${f.root}:${process.env.PATH}`, FAKE_ROOT: f.root,
    SET_ID: f.plan.setId, REVISION: f.plan.revision, GH_TOKEN: 'coordinator', STACK_TOKEN: 'components',
    TARGET_URL: 'https://github.com/run', ...extra}});
}
test('duplicate layer publications dispatch once and do not overwrite result statuses', async () => {
  const f = await fixture();
  const first = run(f, 'dispatch'); assert.equal(first.status, 0, first.stderr);
  const statuses = await readFile(join(f.root, 'statuses'), 'utf8');
  const duplicate = run(f, 'dispatch'); assert.equal(duplicate.status, 0, duplicate.stderr);
  assert.equal(await readFile(join(f.root, 'dispatches'), 'utf8'), 'dispatch\n');
  assert.equal(await readFile(join(f.root, 'statuses'), 'utf8'), statuses);
});
test('a failed dispatch releases its claim and leaves a blocking result', async () => {
  const f = await fixture();
  assert.notEqual(run(f, 'dispatch', {FAIL_DISPATCH: '1'}).status, 0);
  assert.match(await readFile(join(f.root, 'statuses'), 'utf8'), /state=error/);
  const retry = run(f, 'dispatch'); assert.equal(retry.status, 0, retry.stderr);
});
test('stack ancestry is mandatory, and a failed preparation retains all status recipients', async () => {
  const f = await fixture();
  await writeFile(join(f.root, 'targets.json'), JSON.stringify({schemaVersion: 1,
    targets: [{repository, sourceSha: 'b'.repeat(40)}]}));
  const invalid = run(f, 'prepare', {STALE: '1'});
  assert.notEqual(invalid.status, 0); assert.match(invalid.stderr, /must be rebased/);
  assert.equal(JSON.parse(await readFile(join(f.root, 'targets.json'), 'utf8')).statusTargets.length, 2);
  const valid = run(f, 'prepare'); assert.equal(valid.status, 0, valid.stderr);
  const target = JSON.parse(await readFile(join(f.root, 'targets.json'), 'utf8')).targets[0];
  assert.equal(target.baseRef, 'main'); assert.equal(target.baseSha, 'd'.repeat(40));
});
test('obsolete stacks cannot report or dispatch even if the top head is unchanged', async () => {
  const f = await fixture(); const modified = pulls(); modified[0].head.sha = 'c'.repeat(40);
  await writeFile(join(f.root, 'pulls.json'), JSON.stringify(modified));
  for (const mode of ['report', 'dispatch']) {
    const result = run(f, mode); assert.notEqual(result.status, 0); assert.match(result.stderr, /obsolete/);
  }
});
test('workflow routing serializes dispatch and guards exact identities at reporting', async () => {
  const intake = await readFile(new URL('../../.github/workflows/system-test-candidate.yml', import.meta.url), 'utf8');
  assert.match(intake, /group: tpf-stack-dispatch-.*set_id/);
  assert.match(intake, /needs: \[intake, coalesce, dispatch-set\]/);
  const workflow = await readFile(new URL('../../.github/workflows/system-test-compatibility-set.yml', import.meta.url), 'utf8');
  assert.match(workflow, /group: tpf-system-test-set-.*set_id/);
  assert.match(workflow, /cancel-in-progress: true/);
  assert.equal(workflow.match(/coordinate-stack\.mjs --mode report/g).length, 2);
  assert.match(workflow, /statusTargets \/\/ .targets/);
  assert.equal(stackRevision(discovery().stacks), discovery().revision);
});
