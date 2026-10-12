import {createHash} from 'node:crypto';

export function pullStack(pulls, repository, number) {
  const candidates = pulls.filter(p => p.state === 'open' && p.base?.repo?.full_name === repository);
  const byNumber = n => {
    const matches = candidates.filter(p => p.number === n);
    if (matches.length !== 1) throw new Error(`stack PR ${repository}#${n} is missing or ambiguous`);
    return matches[0];
  };
  const sameOwner = (a, b) => a.head?.repo?.owner?.login === b.head?.repo?.owner?.login;
  let root = byNumber(number);
  const seen = new Set();
  while (true) {
    if (seen.has(root.number)) throw new Error('cyclic pull-request stack');
    seen.add(root.number);
    const parents = candidates.filter(p => p.head?.ref === root.base?.ref && sameOwner(p, root));
    if (parents.length > 1) throw new Error('ambiguous stack parent');
    if (!parents.length) break;
    root = parents[0];
  }
  const layers = [];
  seen.clear();
  let current = root;
  while (true) {
    if (seen.has(current.number)) throw new Error('cyclic pull-request stack');
    seen.add(current.number);
    if (!Number.isSafeInteger(current.number) || current.number < 1 ||
        !/^[a-f0-9]{40}$/.test(current.head?.sha) ||
        typeof current.head?.ref !== 'string' || typeof current.base?.ref !== 'string') {
      throw new Error('invalid stack layer identity');
    }
    layers.push({number: current.number, headSha: current.head.sha,
      headRef: current.head.ref, baseRef: current.base.ref});
    const children = candidates.filter(p => p.base?.ref === current.head.ref && sameOwner(p, current));
    if (children.length > 1) throw new Error('branched stack requires explicit selection; refusing to cover sibling PRs');
    if (!children.length) break;
    current = children[0];
  }
  return {repository, rootPullRequestNumber: root.number, rootBaseRef: root.base.ref, layers};
}

export function stackRevision(stacks) {
  return createHash('sha256').update(JSON.stringify([...stacks].sort((a, b) =>
    a.repository.localeCompare(b.repository)))).digest('hex');
}

export function assertCurrentStacks(stacks, pulls) {
  const current = stacks.map(stack => pullStack(pulls, stack.repository, stack.rootPullRequestNumber));
  if (stackRevision(current) !== stackRevision(stacks)) throw new Error('stack changed; result is obsolete');
}
