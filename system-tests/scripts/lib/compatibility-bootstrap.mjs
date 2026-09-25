export function orderedMavenTargets(config, targets) {
  const remaining = new Map([...targets].filter(([name]) => config.components[name].kind === 'maven'));
  const ordered = [];
  while (remaining.size > 0) {
    const ready = [...remaining.values()]
      .filter((target) => Object.keys(config.components[target.component].consumerVersionProperties)
        .every((dependency) => !remaining.has(dependency)))
      .sort((left, right) => left.component.localeCompare(right.component));
    if (ready.length === 0) throw new Error(`compatibility target dependency cycle: ${[...remaining.keys()].sort().join(', ')}`);
    for (const target of ready) {
      ordered.push(target);
      remaining.delete(target.component);
    }
  }
  return ordered;
}

export function expectedCandidateVersion(resolvedSet, target) {
  const reference = resolvedSet.components.contracts?.mavenVersion ?? Object.values(resolvedSet.components)[0]?.mavenVersion;
  const match = typeof reference === 'string' ? /^(\d+\.\d+\.\d+)/.exec(reference) : null;
  if (match === null) throw new Error('baseline does not expose a semantic Maven version');
  return `${match[1]}-pr.${target.pullRequestNumber}.${target.sourceSha.slice(0, 12)}`;
}

export function candidateFromOutput(output) {
  const matches = output.split(/\r?\n/).filter((line) => line.startsWith('candidate=')).map((line) => line.slice('candidate='.length));
  if (matches.length !== 1 || !/^\d+\.\d+\.\d+-pr\.[1-9][0-9]*\.[0-9a-f]{12}$/.test(matches[0])) {
    throw new Error('candidate preparation did not emit exactly one valid version');
  }
  return matches[0];
}
