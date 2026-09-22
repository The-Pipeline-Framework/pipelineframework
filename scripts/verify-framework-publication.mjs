#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const [repository] = process.argv.slice(2);
if (!repository) {
  throw new Error('Usage: verify-framework-publication.mjs <maven-local-repository>');
}

const root = path.resolve(import.meta.dirname, '..');
const frameworkPom = path.join(root, 'framework', 'pom.xml');
const manifestPath = path.join(root, 'framework', 'public-artifacts.json');
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const localRepository = path.resolve(repository);
const failures = [];

const expectedPublic = manifest.publicArtifacts ?? [];
const expectedInternal = manifest.internalArtifacts ?? [];
const expectedExternal = manifest.externalArtifacts ?? [];

if (!Array.isArray(expectedPublic) || !Array.isArray(expectedInternal) || !Array.isArray(expectedExternal)) {
  throw new Error('Manifest must define publicArtifacts, internalArtifacts, and externalArtifacts arrays');
}

function normalizeArtifacts(input, sectionName) {
  const artifacts = new Map();
  for (const entry of input) {
    if (!entry || typeof entry !== 'object' || typeof entry.artifactId !== 'string') {
      failures.push(`${sectionName} entries must be objects with artifactId`);
      continue;
    }

    const artifactId = entry.artifactId.trim();
    if (!artifactId) {
      failures.push(`${sectionName} contains empty artifactId`);
      continue;
    }
    if (artifacts.has(artifactId)) {
      failures.push(`${sectionName} contains duplicate artifactId: ${artifactId}`);
      continue;
    }

    artifacts.set(artifactId, {
      artifactId,
      packaging: typeof entry.packaging === 'string' ? entry.packaging.trim() || 'jar' : 'jar',
    });
  }
  return artifacts;
}

function normalizeStringArtifacts(input, sectionName) {
  const artifacts = new Set();
  for (const item of input) {
    const artifactId =
      typeof item === 'string'
        ? item.trim()
        : item && typeof item === 'object' && typeof item.artifactId === 'string'
          ? item.artifactId.trim()
          : '';

    if (!artifactId) {
      failures.push(`${sectionName} entries must be strings or objects with artifactId`);
      continue;
    }
    if (artifacts.has(artifactId)) {
      failures.push(`${sectionName} contains duplicate artifactId: ${artifactId}`);
      continue;
    }
    artifacts.add(artifactId);
  }
  return artifacts;
}

const publicArtifacts = normalizeArtifacts(expectedPublic, 'publicArtifacts');
const internalArtifacts = normalizeStringArtifacts(expectedInternal, 'internalArtifacts');
const externalArtifacts = normalizeStringArtifacts(expectedExternal, 'externalArtifacts');
const externalSourceMirrors = new Set();
for (const entry of expectedExternal) {
  if (entry && typeof entry === 'object' && 'reactorSourceMirror' in entry) {
    if (typeof entry.reactorSourceMirror !== 'boolean') {
      failures.push(`external artifact ${entry.artifactId} has non-boolean reactorSourceMirror`);
    } else if (entry.reactorSourceMirror && typeof entry.artifactId === 'string') {
      externalSourceMirrors.add(entry.artifactId.trim());
    }
  }
}
const publicArtifactIds = new Set(publicArtifacts.keys());
const allDeclared = new Set([...publicArtifactIds, ...internalArtifacts, ...externalArtifacts]);
let centralExcludedArtifacts = new Set();

if (allDeclared.size !== publicArtifactIds.size + internalArtifacts.size + externalArtifacts.size) {
  failures.push('public/internal/external artifact declarations overlap');
}

function effectiveProjects() {
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'tpf-publication-'));
  const effectivePom = path.join(temporaryDirectory, 'effective-pom.xml');

  try {
    const result = spawnSync(
      path.join(root, 'mvnw'),
      [
        '-q',
        '-f',
        frameworkPom,
        '-Pcentral-publishing',
        '-DskipTests',
        'help:effective-pom',
        `-Doutput=${effectivePom}`,
        `-Dmaven.repo.local=${localRepository}`,
      ],
      {
        cwd: root,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        maxBuffer: 16 * 1024 * 1024,
      },
    );

    if (result.status !== 0 || !fs.existsSync(effectivePom)) {
      const output = `${result.stderr || ''}\n${result.stdout || ''}`.trim();
      const reason = output || `exit code ${result.status}`;
      throw new Error(`failed to render the central-publishing effective POM: ${reason}`);
    }

    const effectiveXml = fs.readFileSync(effectivePom, 'utf8');
    centralExcludedArtifacts = centralExclusions(effectiveXml);
    return parseEffectiveProjects(effectiveXml);
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

function centralExclusions(effectivePom) {
  const projectPattern = /<project(?:\s[^>]*)?>([\s\S]*?)<\/project>/g;
  const frameworkProject = [...effectivePom.matchAll(projectPattern)]
    .map((match) => match[1].replace(/<parent>[\s\S]*?<\/parent>/, ''))
    .find((project) => elementValue(project, 'artifactId') === 'framework-parent');
  if (!frameworkProject) return new Set();

  const pluginId = '<artifactId>central-publishing-maven-plugin</artifactId>';
  const start = frameworkProject.indexOf(pluginId);
  const end = frameworkProject.indexOf('</plugin>', start);
  if (start < 0 || end < 0) return new Set();
  const plugin = frameworkProject.slice(start, end);
  const exclusions = plugin.match(/<excludeArtifacts>([\s\S]*?)<\/excludeArtifacts>/)?.[1] ?? '';
  return new Set([...exclusions.matchAll(/<excludeArtifact>([^<]+)<\/excludeArtifact>/g)]
    .map((match) => match[1].trim()));
}

function parseEffectiveProjects(effectivePom) {
  const projects = new Map();
  const projectPattern = /<project(?:\s[^>]*)?>([\s\S]*?)<\/project>/g;

  for (const match of effectivePom.matchAll(projectPattern)) {
    const project = match[1].replace(/<parent>[\s\S]*?<\/parent>/, '');
    const groupId = elementValue(project, 'groupId');
    if (groupId !== manifest.groupId) continue;

    const artifactId = elementValue(project, 'artifactId');
    if (!artifactId) {
      failures.push('effective POM contains a framework project without artifactId');
      continue;
    }
    if (projects.has(artifactId)) {
      failures.push(`reactor artifact appears multiple times: ${artifactId}`);
      continue;
    }

    const properties = project.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
    const deploySkip = elementValue(properties, 'maven.deploy.skip');
    if (deploySkip !== 'true' && deploySkip !== 'false') {
      failures.push(`effective maven.deploy.skip for ${artifactId} is not boolean: ${deploySkip || '<unset>'}`);
    }

    const centralPluginId = '<artifactId>central-publishing-maven-plugin</artifactId>';
    const centralStart = project.lastIndexOf(centralPluginId);
    const centralEnd = project.indexOf('</plugin>', centralStart);
    const centralPlugin = centralStart >= 0 && centralEnd >= 0
      ? project.slice(centralStart, centralEnd)
      : '';
    const exclusions = centralPlugin.match(/<excludeArtifacts>([\s\S]*?)<\/excludeArtifacts>/)?.[1] ?? '';
    const centralExcluded = [...exclusions.matchAll(/<excludeArtifact>([^<]+)<\/excludeArtifact>/g)]
      .some((match) => match[1].trim() === artifactId);

    projects.set(artifactId, {
      artifactId,
      effectiveProject: project,
      packaging: elementValue(project, 'packaging') || 'jar',
      deployable: deploySkip === 'false',
      centralPresent: Boolean(centralPlugin),
      centralSkipped: elementValue(centralPlugin, 'skipPublishing') === 'true',
      centralExcluded,
      dependencies: directDependencies(project),
    });
  }

  return projects;
}

function verifyBomCoordinates(effectiveProject) {
  const initialFailureCount = failures.length;
  const bomPom = fs.readFileSync(path.join(root, 'framework', 'bom', 'pom.xml'), 'utf8');
  const managedSection = bomPom.match(/<dependencyManagement>([\s\S]*?)<\/dependencyManagement>/)?.[1] ?? '';
  const dependencyPattern = /<dependency>([\s\S]*?)<\/dependency>/g;
  const propertiesSection = effectiveProject.match(/<properties>([\s\S]*?)<\/properties>/)?.[1] ?? '';
  const properties = new Map(
    [...propertiesSection.matchAll(/<([A-Za-z0-9_.-]+)>([^<]+)<\/\1>/g)]
      .map((match) => [match[1], match[2].trim()]),
  );
  const managed = [...managedSection.matchAll(dependencyPattern)].map((match) => {
    const dependency = match[1];
    return {
      groupId: resolveProperties(elementValue(dependency, 'groupId'), properties),
      artifactId: resolveProperties(elementValue(dependency, 'artifactId'), properties),
      version: resolveProperties(elementValue(dependency, 'version'), properties),
      type: resolveProperties(elementValue(dependency, 'type') || 'jar', properties),
      classifier: resolveProperties(elementValue(dependency, 'classifier'), properties),
    };
  });

  for (const dependency of managed) {
    if (!dependency.groupId || !dependency.artifactId || !dependency.version || !dependency.type) {
      failures.push(
        `BOM contains an incomplete managed coordinate: ${dependency.groupId || '<groupId>'}:` +
          `${dependency.artifactId || '<artifactId>'}:${dependency.type || '<type>'}:` +
          `${dependency.version || '<version>'}`,
      );
    } else if (Object.values(dependency).some((value) => /\$\{[^}]+}/.test(value))) {
      failures.push(
        `BOM contains an unresolved managed coordinate: ${dependency.groupId}:` +
          `${dependency.artifactId}:${dependency.type}:${dependency.version}`,
      );
    }
  }
  if (!managed.length || failures.length > initialFailureCount) return;

  const repositories = effectiveProject.match(/<repositories>([\s\S]*?)<\/repositories>/)?.[0] ?? '';
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'tpf-bom-availability-'));
  const verificationPom = path.join(temporaryDirectory, 'pom.xml');
  const isolatedRepository = path.join(temporaryDirectory, 'repository');

  try {
    // Resolve only the advertised files. Transitive graphs are not part of the BOM availability contract.
    const executions = managed.map((dependency, index) => {
      const coordinate = [
        dependency.groupId,
        dependency.artifactId,
        dependency.version,
        dependency.type,
        dependency.classifier,
      ].filter(Boolean).join(':');
      return `
        <execution>
          <id>verify-managed-artifact-${index}</id>
          <phase>validate</phase>
          <goals><goal>get</goal></goals>
          <configuration>
            <artifact>${escapeXml(coordinate)}</artifact>
            <transitive>false</transitive>
          </configuration>
        </execution>`;
    }).join('');
    fs.writeFileSync(
      verificationPom,
      `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.pipelineframework.verification</groupId>
  <artifactId>pipelineframework-bom-availability</artifactId>
  <version>1</version>
  ${repositories}
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-dependency-plugin</artifactId>
        <version>3.9.0</version>
        <executions>${executions}
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
`,
    );

    const result = spawnSync(
      path.join(root, 'mvnw'),
      [
        '-q',
        '-f',
        verificationPom,
        'validate',
        // A fresh cache prevents locally installed or previously downloaded artifacts from hiding publication gaps.
        `-Dmaven.repo.local=${isolatedRepository}`,
      ],
      {
        cwd: root,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        maxBuffer: 32 * 1024 * 1024,
      },
    );

    if (result.status !== 0) {
      const output = `${result.stderr || ''}\n${result.stdout || ''}`.trim();
      failures.push(
        `BOM managed artifact availability check failed against configured repositories:\n` +
          `${output || `Maven exited with code ${result.status}`}`,
      );
    }
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

function resolveProperties(value, properties) {
  return value.replace(/\$\{([^}]+)}/g, (expression, property) => properties.get(property) ?? expression);
}

function escapeXml(value) {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function directDependencies(project) {
  const section = project.match(/^    <dependencies>\s*$([\s\S]*?)^    <\/dependencies>\s*$/m)?.[1] ?? '';
  const dependencies = [];
  const dependencyPattern = /^      <dependency>\s*$([\s\S]*?)^      <\/dependency>\s*$/gm;

  for (const match of section.matchAll(dependencyPattern)) {
    const dependency = match[1];
    dependencies.push({
      groupId: elementValue(dependency, 'groupId'),
      artifactId: elementValue(dependency, 'artifactId'),
      scope: elementValue(dependency, 'scope') || 'compile',
    });
  }
  return dependencies;
}

function elementValue(xml, element) {
  const match = xml.match(new RegExp(`<${element}>([^<]+)</${element}>`));
  return match?.[1]?.trim() ?? '';
}

const reactorArtifacts = effectiveProjects();
const bomArtifact = reactorArtifacts.get('pipelineframework-bom');
if (bomArtifact) {
  verifyBomCoordinates(bomArtifact.effectiveProject);
}

for (const artifactId of centralExcludedArtifacts) {
  if (!internalArtifacts.has(artifactId) && !externalSourceMirrors.has(artifactId)) {
    failures.push(`Central bundle excludes an artifact outside the internal/source-mirror contract: ${artifactId}`);
  }
}
for (const artifactId of internalArtifacts) {
  if (!centralExcludedArtifacts.has(artifactId)) {
    failures.push(`internal artifact is missing from Central bundle exclusions: ${artifactId}`);
  }
}

for (const [artifactId, artifact] of reactorArtifacts) {
  if (!allDeclared.has(artifactId)) {
    failures.push(`undeclared reactor artifact: ${artifactId}`);
    continue;
  }

  if (externalArtifacts.has(artifactId)) {
    if (!externalSourceMirrors.has(artifactId)) {
      failures.push(`externally owned artifact is still present in this reactor: ${artifactId}`);
    }
    if (artifact.deployable) {
      failures.push(`externally owned artifact is deployable: ${artifactId}`);
    }
    if (externalSourceMirrors.has(artifactId) && !artifact.centralExcluded) {
      failures.push(`externally owned source mirror is included in Central bundle: ${artifactId}`);
    }
  }
  if (internalArtifacts.has(artifactId) && artifact.deployable) {
    failures.push(`internal artifact is deployable: ${artifactId}`);
  }
  if (internalArtifacts.has(artifactId) && !artifact.centralExcluded && !artifact.centralSkipped) {
    failures.push(`internal artifact is included in Central bundle: ${artifactId}`);
  }
  if (publicArtifactIds.has(artifactId) && !artifact.deployable) {
    failures.push(`declared public artifact is not deployable: ${artifactId}`);
  }
  if (publicArtifactIds.has(artifactId) &&
      (!artifact.centralPresent || artifact.centralSkipped || artifact.centralExcluded)) {
    failures.push(`declared public artifact is not publishable by Central plugin: ${artifactId}`);
  }
  if (publicArtifactIds.has(artifactId)) {
    for (const dependency of artifact.dependencies) {
      if (
        dependency.groupId === manifest.groupId &&
        dependency.scope !== 'test' &&
        !publicArtifactIds.has(dependency.artifactId) &&
        !externalArtifacts.has(dependency.artifactId)
      ) {
        failures.push(
          `public artifact ${artifactId} has a non-public ${dependency.scope} dependency: ${dependency.artifactId}`,
        );
      }
    }
  }

  const expectedPackaging = publicArtifacts.get(artifactId)?.packaging;
  if (expectedPackaging && expectedPackaging !== artifact.packaging) {
    failures.push(
      `public artifact packaging drift for ${artifactId}: expected ${expectedPackaging}, found ${artifact.packaging}`,
    );
  }
}

for (const artifactId of publicArtifactIds) {
  if (!reactorArtifacts.has(artifactId)) {
    failures.push(`declared public artifact is not in reactor: ${artifactId}`);
  }
}

for (const artifactId of internalArtifacts) {
  if (!reactorArtifacts.has(artifactId)) {
    failures.push(`declared internal artifact is not in reactor: ${artifactId}`);
  }
}

if (failures.length) {
  throw new Error(failures.join('\n'));
}

console.log(
  `verified publication contract: ${publicArtifactIds.size} public, ${internalArtifacts.size} internal, ${externalArtifacts.size} external artifacts; ${reactorArtifacts.size} reactor projects classified`,
);
