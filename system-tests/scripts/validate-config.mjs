#!/usr/bin/env node
import { readJson, validateComponentsConfig, validatePolicy } from './lib/contracts.mjs';

const componentsPath = process.argv[2] ?? 'system-tests/components.yml';
const policyPath = process.argv[3] ?? 'system-tests/policy.yml';
const config = validateComponentsConfig(await readJson(componentsPath));
validatePolicy(await readJson(policyPath), config);
process.stdout.write(`Validated ${Object.keys(config.components).length} components and the suite policy.\n`);
