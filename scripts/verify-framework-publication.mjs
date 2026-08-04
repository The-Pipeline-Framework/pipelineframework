#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';

const [repository] = process.argv.slice(2);
if (!repository) {
  throw new Error('Usage: verify-framework-publication.mjs <file-repository>');
}

const root = path.resolve(import.meta.dirname, '..');
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'framework/public-artifacts.json'), 'utf8'));
const version = process.env.TPF_PUBLICATION_VERSION;
if (!version) {
  throw new Error('TPF_PUBLICATION_VERSION must name the staged Maven version');
}

const failures = [];
if (manifest.artifacts.length !== 13) failures.push(`expected 13 public coordinates, found ${manifest.artifacts.length}`);
if (manifest.internalArtifacts.length !== 4) failures.push(`expected fixture plus three structural aggregators, found ${manifest.internalArtifacts.length}`);
if (manifest.reactorProjectCount !== manifest.artifacts.length + manifest.internalArtifacts.length) {
  failures.push('R must equal M union F, with M and F disjoint');
}
const declaredArtifactIds = new Set([
  ...manifest.artifacts.map(({ artifactId }) => artifactId),
  ...manifest.internalArtifacts
]);
const groupDirectory = path.join(repository, ...manifest.groupId.split('.'));
if (fs.existsSync(groupDirectory)) {
  for (const entry of fs.readdirSync(groupDirectory, { withFileTypes: true })) {
    if (
      entry.isDirectory() &&
      !declaredArtifactIds.has(entry.name) &&
      fs.existsSync(path.join(groupDirectory, entry.name, version))
    ) {
      failures.push(`undeclared artifact deployed: ${entry.name}`);
    }
  }
}
for (const artifact of manifest.artifacts) {
  const artifactDirectory = path.join(repository, ...manifest.groupId.split('.'), artifact.artifactId, version);
  const required = [`${artifact.artifactId}-${version}.pom`, `${artifact.artifactId}-${version}.pom.asc`];
  if (artifact.packaging === 'jar') {
    required.push(
      `${artifact.artifactId}-${version}.jar`,
      `${artifact.artifactId}-${version}.jar.asc`,
      `${artifact.artifactId}-${version}-sources.jar`,
      `${artifact.artifactId}-${version}-sources.jar.asc`,
      `${artifact.artifactId}-${version}-javadoc.jar`,
      `${artifact.artifactId}-${version}-javadoc.jar.asc`
    );
  }
  for (const file of required) {
    if (!fs.existsSync(path.join(artifactDirectory, file))) failures.push(`missing ${artifact.artifactId}: ${file}`);
  }
}
for (const artifact of manifest.internalArtifacts) {
  const artifactDirectory = path.join(repository, ...manifest.groupId.split('.'), artifact, version);
  if (fs.existsSync(artifactDirectory)) failures.push(`internal artifact deployed: ${artifact}`);
}
if (failures.length) throw new Error(failures.join('\n'));
console.log(`verified ${manifest.artifacts.length} public artifacts; pom packaging requires only POM + signature`);
