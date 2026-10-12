import {createHash} from 'node:crypto';
import {pullStack, stackRevision} from './pull-stack.mjs';

const SET_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

export function discoverCompatibilitySet(config, pulls, {
  sourceRepository,
  sourcePullRequest,
  sourceSha,
  requestedSetId = null
}) {
  if (!Array.isArray(pulls)) throw new Error('pull request discovery input must be an array');
  const repositories = Object.values(config.components).map(({repository}) => repository);
  if (!repositories.includes(sourceRepository)) throw new Error(`source repository is not an allowed component owner: ${sourceRepository}`);

  const sourceMatches = pulls.filter((pull) =>
    pull?.base?.repo?.full_name === sourceRepository && pull?.number === sourcePullRequest);
  if (sourceMatches.length !== 1) {
    throw new Error(`source pull request must appear exactly once; found ${sourceMatches.length}`);
  }
  const source = sourceMatches[0];
  if (source.state !== 'open') throw new Error('source pull request is not open');
  if (source.head?.sha !== sourceSha) throw new Error('source pull request head moved after candidate publication');
  const headOwner = source.head?.repo?.owner?.login;
  const sourceStack = pullStack(pulls, sourceRepository, sourcePullRequest);
  const headRef = sourceStack.layers[0].headRef;
  const baseRef = sourceStack.rootBaseRef;
  for (const [value, name] of [[headOwner, 'head owner'], [headRef, 'head ref'], [baseRef, 'base ref']]) {
    if (typeof value !== 'string' || value.length === 0) throw new Error(`source pull request ${name} is missing`);
  }

  const selected = [];
  const stacks = [];
  for (const repository of repositories) {
    const matches = pulls.filter((pull) =>
      pull?.state === 'open' &&
      pull?.base?.repo?.full_name === repository &&
      pull?.base?.ref === baseRef &&
      pull?.head?.repo?.owner?.login === headOwner &&
      pull?.head?.ref === headRef);
    if (matches.length > 1) throw new Error(`multiple open ${repository} pull requests share ${headOwner}:${headRef}`);
    if (matches.length === 1) {
      const number = matches[0].number;
      if (!Number.isSafeInteger(number) || number < 1) throw new Error(`discovered ${repository} pull request number is invalid`);
      const stack = pullStack(pulls, repository, number);
      stacks.push(stack);
      selected.push(`https://github.com/${repository}/pull/${stack.layers.at(-1).number}`);
    }
  }

  if (selected.length < 2 && sourceStack.layers.length < 2) return {coalesced: false, setId: null, pullRequests: selected};
  const identity = selected.length === 1 ? `${sourceRepository}\0${sourceStack.rootPullRequestNumber}` : `${headOwner}\0${headRef}`;
  const setId = requestedSetId ?? `auto-${createHash('sha256').update(identity).digest('hex').slice(0, 20)}`;
  if (!SET_ID_PATTERN.test(setId)) throw new Error('compatibility set ID is invalid');
  return {coalesced: true, setId, pullRequests: selected, stacks, revision: stackRevision(stacks)};
}
