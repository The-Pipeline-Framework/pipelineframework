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

const fs = require('fs');
const os = require('os');
const path = require('path');
const YAML = require('js-yaml');
const PipelineGenerator = require('../src/pipeline-generator');

describe('PipelineGenerator v2', () => {
  let tempDir;

  afterEach(() => {
    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
      tempDir = null;
    }
  });

  test('generateSampleConfig emits v2 semantic messages', async () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const outputPath = path.join(tempDir, 'sample.yaml');

    await generator.generateSampleConfig(outputPath);

    const config = YAML.load(fs.readFileSync(outputPath, 'utf8'));
    expect(config.version).toBe(2);
    expect(config.messages.CustomerInput.fields[0].type).toBe('uuid');
    expect(config.messages.ValidationOutput.fields[1].type).toBe('bool');
    expect(config.steps[0].inputTypeName).toBe('CustomerInput');
    expect(config.steps[0].outputTypeName).toBe('CustomerOutput');
  });

  test('toScaffoldConfig derives legacy field bindings from v2 messages', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {
        ChargeRequest: {
          fields: [
            { number: 1, name: 'orderId', type: 'uuid' },
            { number: 2, name: 'amount', type: 'decimal' }
          ]
        },
        ChargeResult: {
          fields: [
            { number: 1, name: 'paymentId', type: 'uuid' },
            { number: 2, name: 'auditTrail', type: 'string', repeated: true }
          ]
        }
      },
      steps: [
        {
          name: 'Charge Card',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'ChargeRequest',
          outputTypeName: 'ChargeResult'
        }
      ]
    };

    const scaffold = generator.toScaffoldConfig(config);
    const step = scaffold.steps[0];

    expect(step.inputFields[0].type).toBe('UUID');
    expect(step.inputFields[0].protoType).toBe('string');
    expect(step.inputFields[1].type).toBe('BigDecimal');
    expect(step.outputFields[1].type).toBe('List<String>');
    expect(step.outputFields[1].protoType).toBe('string');
  });
});
