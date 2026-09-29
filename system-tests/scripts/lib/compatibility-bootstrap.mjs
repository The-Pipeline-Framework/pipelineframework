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

export function expectedCandidateVersion(resolvedSet, target, sourcePom) {
  if (sourcePom !== undefined) {
    const activePom = sourcePom.replace(/<!--[\s\S]*?-->/g, '');
    const project = activePom.match(/<project\b[^>]*>([\s\S]*?)<\/project\s*>/)?.[1] ?? '';
    let depth = 0;
    let projectVersion;
    // Only direct children count; parent and dependency versions are excluded.
    for (const tag of project.matchAll(/<(\/?)([\w:.-]+)\b[^>]*>/g)) {
      if (tag[1] === '/') depth -= 1;
      else {
        if (depth === 0 && tag[2] === 'version') {
          projectVersion = project.slice(tag.index + tag[0].length).match(/^([^<]*)<\/version\s*>/)?.[1];
          break;
        }
        if (!tag[0].endsWith('/>')) depth += 1;
      }
    }
    const sourceMatch = typeof projectVersion === 'string' ? /^(\d+\.\d+\.\d+)-SNAPSHOT$/.exec(projectVersion.trim()) : null;
    if (sourceMatch === null) throw new Error('root project version must be a semantic SNAPSHOT version');
    return target.pullRequestNumber === null
      ? `${sourceMatch[1]}-main.${target.sourceSha.slice(0, 12)}`
      : `${sourceMatch[1]}-pr.${target.pullRequestNumber}.${target.sourceSha.slice(0, 12)}`;
  }
  const baselineReference = resolvedSet.components.contracts?.mavenVersion ?? Object.values(resolvedSet.components)[0]?.mavenVersion;
  const baselineMatch = typeof baselineReference === 'string' ? /^(\d+\.\d+\.\d+)/.exec(baselineReference) : null;
  if (baselineMatch === null) throw new Error('baseline does not expose a semantic Maven version');
  return target.pullRequestNumber === null
    ? `${baselineMatch[1]}-main.${target.sourceSha.slice(0, 12)}`
    : `${baselineMatch[1]}-pr.${target.pullRequestNumber}.${target.sourceSha.slice(0, 12)}`;
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

export function pinDependencyProperties(sourcePom, dependencyVersions) {
  let pinned = sourcePom;
  for (const [property, version] of Object.entries(dependencyVersions).sort(([left], [right]) => left.localeCompare(right))) {
    if (!/^[A-Za-z0-9_.-]+$/.test(property)) throw new Error(`dependency property is invalid: ${property}`);
    if (typeof version !== 'string' || version.length === 0 || /SNAPSHOT/i.test(version)) {
      throw new Error(`dependency ${property} must use an immutable version`);
    }
    const escaped = property.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(`(<${escaped}>\\s*)[^<]*(\\s*</${escaped}>)`, 'g');
    const activePom = pinned.replace(/<!--[\s\S]*?-->/g, (comment) => ' '.repeat(comment.length));
    const matches = [...activePom.matchAll(pattern)];
    if (matches.length !== 1) {
      throw new Error(`POM must declare ${property} exactly once; found ${matches.length}`);
    }
    const [match] = matches;
    pinned = `${pinned.slice(0, match.index)}${match[1]}${version}${match[2]}${pinned.slice(match.index + match[0].length)}`;
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
