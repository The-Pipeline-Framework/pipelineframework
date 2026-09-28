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
  return target.pullRequestNumber === null
    ? `${match[1]}-main.${target.sourceSha.slice(0, 12)}`
    : `${match[1]}-pr.${target.pullRequestNumber}.${target.sourceSha.slice(0, 12)}`;
}

export function augmentCompatibilityTargets(config, baseline, targetDocument, currentHeads) {
  const targets = new Map(targetDocument.targets.map((target) => [target.component, structuredClone(target)]));
  const selectedMaven = new Set(
    [...targets.values()]
      .filter((target) => config.components[target.component].kind === 'maven')
      .map((target) => target.component)
  );

  for (const [component, definition] of Object.entries(config.components)) {
    const current = currentHeads[component];
    if (current === undefined) throw new Error(`current main head is missing for ${component}`);
    const baselineEntry = definition.kind === 'maven' ? baseline.components[component] : baseline.testHarnesses[component];
    if (baselineEntry === undefined) throw new Error(`baseline entry is missing for ${component}`);
    if (current.sha !== baselineEntry.sha && definition.kind === 'maven') selectedMaven.add(component);
    if (current.sha !== baselineEntry.sha && definition.kind === 'source' && !targets.has(component)) {
      targets.set(component, mainTarget(component, definition, current));
    }
  }

  let changed = true;
  while (changed) {
    changed = false;
    for (const [component, definition] of Object.entries(config.components)) {
      if (definition.kind !== 'maven' || selectedMaven.has(component)) continue;
      const dependencies = Object.keys(definition.consumerVersionProperties);
      if (dependencies.some((dependency) => selectedMaven.has(dependency))) {
        selectedMaven.add(component);
        changed = true;
      }
    }
  }

  for (const component of selectedMaven) {
    if (targets.has(component)) continue;
    targets.set(component, mainTarget(component, config.components[component], currentHeads[component]));
  }
  return {schemaVersion: 1, targets: [...targets.values()].sort((left, right) => left.component.localeCompare(right.component))};
}

function mainTarget(component, definition, current) {
  return {
    component,
    repository: definition.repository,
    repositoryName: definition.repository.split('/')[1],
    baseRef: current.baseRef,
    pullRequestNumber: null,
    sourceSha: current.sha,
    baseSha: current.sha,
    merged: true,
    testedSha: current.sha
  };
}

export function candidateBuildArguments(mavenRepository, versionArguments) {
  return [
    '-B', 'install', '--no-transfer-progress',
    '-Dmaven.deploy.skip=true', '-Dgpg.skip=true', '-Dtpf.flatten.skip=true',
    '-DskipTests=true', '-DskipITs=true', '-DskipUnitTests=true', '-Dinvoker.skip=true',
    `-Dmaven.repo.local=${mavenRepository}`,
    ...versionArguments
  ];
}

export function pinCandidateDependencyProperties(sourcePom, dependencyVersions) {
  let pinned = sourcePom;
  for (const [property, version] of Object.entries(dependencyVersions).sort(([left], [right]) => left.localeCompare(right))) {
    if (!/^[A-Za-z0-9_.-]+$/.test(property)) throw new Error(`candidate dependency property is invalid: ${property}`);
    if (typeof version !== 'string' || version.length === 0 || /SNAPSHOT/i.test(version)) {
      throw new Error(`candidate dependency ${property} must use an immutable version`);
    }
    const escaped = property.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(`(<${escaped}>\\s*)[^<]*(\\s*</${escaped}>)`, 'g');
    const matches = [...pinned.matchAll(pattern)];
    if (matches.length !== 1) {
      throw new Error(`candidate POM must declare ${property} exactly once; found ${matches.length}`);
    }
    pinned = pinned.replace(pattern, (_match, opening, closing) => `${opening}${version}${closing}`);
  }
  return pinned;
}

export function candidateFromOutput(output) {
  const matches = output.split(/\r?\n/).filter((line) => line.startsWith('candidate=')).map((line) => line.slice('candidate='.length));
  if (matches.length !== 1 || !/^\d+\.\d+\.\d+-(?:pr\.[1-9][0-9]*|main)\.[0-9a-f]{12}$/.test(matches[0])) {
    throw new Error('candidate preparation did not emit exactly one valid version');
  }
  return matches[0];
}
