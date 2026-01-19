/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Script to rebuild the browser bundle with templates and browser engine
// This ensures the web UI stays in sync with the Node.js generator

import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const nodeGeneratorPath = path.join(__dirname, '../template-generator-node');
const templatesDir = path.join(nodeGeneratorPath, 'templates');
const browserEnginePath = path.join(nodeGeneratorPath, 'src/browser-template-engine.js');
const webUiBundlePath = path.join(__dirname, 'static/browser-bundle.js');

function readTemplates() {
  let templateFiles;
  try {
    templateFiles = fs.readdirSync(templatesDir);
  } catch (error) {
    console.error(`Error reading templates directory '${templatesDir}': ${error.message}`);
    process.exit(1);
  }

  const templates = {};
  for (const file of templateFiles) {
    if (!file.endsWith('.hbs')) {
      continue;
    }
    try {
      const templateName = path.basename(file, '.hbs');
      const templateContent = fs.readFileSync(path.join(templatesDir, file), 'utf8');
      templates[templateName] = templateContent;
    } catch (error) {
      console.error(`Error reading template file ${file}: ${error.message}`);
      process.exit(1);
    }
  }

  if (Object.keys(templates).length === 0) {
    console.error(`Warning: No .hbs template files found in directory '${templatesDir}'`);
    process.exit(1);
  }

  return templates;
}

function readBrowserEngine() {
  try {
    return fs.readFileSync(browserEnginePath, 'utf8');
  } catch (error) {
    console.error(`Error reading browser engine file '${browserEnginePath}': ${error.message}`);
    process.exit(1);
  }
}

function patchEngineTemplates(engineCode) {
  const templateDefaultRegex = /this\.templates\s*=\s*templates\s*\|\|\s*\{\};/;
  if (templateDefaultRegex.test(engineCode)) {
    return engineCode.replace(templateDefaultRegex, 'this.templates = templates || TEMPLATES;');
  }
  if (engineCode.includes('templates || TEMPLATES')) {
    return engineCode;
  }
  console.error('Could not set default templates reference in browser engine code.');
  process.exit(1);
}

const templates = readTemplates();
const engineCode = patchEngineTemplates(readBrowserEngine());

const header = `/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This is a combined bundle for the browser-based template generator
// It includes Handlebars, the templates, and the browser engine

// The templates are embedded as a JS object
`;

const templatesJsContent = `const TEMPLATES = ${JSON.stringify(templates, null, 2)};`;

const bundleContent = `${header}${templatesJsContent}

// Handlebars template engine for the browser
(function(global, undefined) {
  "use strict";

${engineCode}
})(this);
`;

try {
  fs.writeFileSync(webUiBundlePath, bundleContent);
} catch (error) {
  console.error(`Error writing browser bundle file '${webUiBundlePath}': ${error.message}`);
  process.exit(1);
}

console.log('Browser bundle has been rebuilt with templates and engine code.');
