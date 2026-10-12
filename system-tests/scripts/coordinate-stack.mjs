#!/usr/bin/env node
import {execFileSync} from 'node:child_process';
import {readFile, writeFile} from 'node:fs/promises';
import {parseArgs} from 'node:util';
import {assertCurrentStacks, stackRevision} from './lib/pull-stack.mjs';

const {values} = parseArgs({options: {mode: {type: 'string'}, plan: {type: 'string'},
  targets: {type: 'string'}, coordinator: {type: 'string'}, coordinatorSha: {type: 'string'}}, strict: true});
const plan = JSON.parse(await readFile(values.plan, 'utf8'));
const stacks = plan.stacks ?? plan.stackSnapshot ?? [];
const revision = plan.revision ?? plan.stackRevision;
if (stacks.length && stackRevision(stacks) !== revision) throw new Error('invalid immutable stack revision');
const api = (path, args = [], token = process.env.STACK_TOKEN) => JSON.parse(execFileSync('gh',
  ['api', path, ...args], {encoding: 'utf8', env: {...process.env, GH_TOKEN: token}}) || '{}');
async function current() {
  const pulls = stacks.flatMap(stack => api(`repos/${stack.repository}/pulls?state=open&per_page=100`, ['--paginate', '--slurp']).flat());
  assertCurrentStacks(stacks, pulls);
}
function status(state, description, targetUrl) {
  for (const stack of stacks) for (const layer of stack.layers) {
    api(`repos/${stack.repository}/statuses/${layer.headSha}`, ['--method', 'POST',
      '-f', `state=${state}`, '-f', 'context=tpf/system-tests', '-f', `description=${description}`,
      '-f', `target_url=${targetUrl}`]);
  }
}
if (values.mode === 'dispatch') {
  await current();
  const ref = `tags/tpf-system-test-stack/${plan.setId}/${revision}`;
  const coordinatorToken = process.env.GH_TOKEN;
  // Atomic creation prevents two publications from claiming the same revision.
  try {
    api(`repos/${values.coordinator}/git/refs`, ['--method', 'POST',
      '-f', `ref=refs/${ref}`, '-f', `sha=${values.coordinatorSha}`], coordinatorToken);
  } catch (failure) {
    // Only an exact existing claim is a duplicate; permission/network failures propagate.
    const existing = api(`repos/${values.coordinator}/git/ref/${ref}`, [], coordinatorToken);
    if (existing.ref !== `refs/${ref}`) throw failure;
    console.log(`Stack revision ${revision} already dispatched; no duplicate test run`);
    process.exit(0);
  }
  try {
    status('pending', 'Stack changed; awaiting exact top-of-stack product tests', process.env.TARGET_URL);
    const input = JSON.stringify({ref: 'main', inputs: {set_id: plan.setId,
      pull_requests: plan.pullRequests.join('\n'), stack_layers: JSON.stringify(stacks), stack_revision: revision}});
    execFileSync('gh', ['api', `repos/${values.coordinator}/actions/workflows/system-test-compatibility-set.yml/dispatches`,
      '--method', 'POST', '--input', '-'], {input, encoding: 'utf8', env: process.env});
  } catch (failure) {
    api(`repos/${values.coordinator}/git/refs/${ref}`, ['--method', 'DELETE'], coordinatorToken);
    status('error', 'Stack dispatch failed; candidate intake must be retried', process.env.TARGET_URL);
    throw failure;
  }
} else if (values.mode === 'prepare') {
  const targets = JSON.parse(await readFile(values.targets, 'utf8'));
  if (stacks.length) {
    targets.stackSnapshot = stacks;
    targets.stackRevision = revision;
    targets.statusTargets = stacks.flatMap(stack => stack.layers.map(layer =>
      ({repository: stack.repository, sourceSha: layer.headSha})));
    // Persist status recipients before ancestry validation, so failure remains blocking.
    await writeFile(values.targets, JSON.stringify(targets, null, 2));
    await current();
    for (const stack of stacks) {
      for (let index = 1; index < stack.layers.length; index++) {
        const parent = stack.layers[index - 1].headSha, child = stack.layers[index].headSha;
        const comparison = api(`repos/${stack.repository}/compare/${parent}...${child}`);
        if (comparison.merge_base_commit?.sha !== parent || !['ahead', 'identical'].includes(comparison.status)) {
          throw new Error(`Stack PR #${stack.layers[index].number} must be rebased: it does not contain current parent #${stack.layers[index - 1].number}`);
        }
      }
      const target = targets.targets.find(t => t.repository === stack.repository);
      if (!target || target.sourceSha !== stack.layers.at(-1).headSha) throw new Error('resolved top head differs from stack snapshot');
      target.baseRef = stack.rootBaseRef;
      target.baseSha = api(`repos/${stack.repository}/commits/${encodeURIComponent(stack.rootBaseRef)}`).sha;
    }
    await writeFile(values.targets, JSON.stringify(targets, null, 2));
  }
} else if (values.mode === 'report') {
  if (stacks.length) await current();
} else throw new Error('unknown stack coordination mode');
