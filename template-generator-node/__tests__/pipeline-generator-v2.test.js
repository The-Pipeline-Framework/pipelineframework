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

  test('loadConfig accepts remote execution metadata for v2 steps', () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'remote-config.yaml');
    fs.writeFileSync(configPath, `version: 2
appName: TestApp
basePackage: com.example.test
transport: REST
runtimeLayout: MODULAR
messages:
  ChargeRequest:
    fields:
      - number: 1
        name: orderId
        type: uuid
  ChargeResult:
    fields:
      - number: 1
        name: paymentId
        type: uuid
steps:
  - name: Charge Card
    cardinality: ONE_TO_ONE
    inputTypeName: ChargeRequest
    outputTypeName: ChargeResult
    execution:
      mode: REMOTE
      operatorId: charge-card
      protocol: PROTOBUF_HTTP_V1
      timeoutMs: 3000
      target:
        urlConfigKey: tpf.remote-operators.charge-card.url
`);

    const config = generator.loadConfig(configPath);
    expect(config.steps[0].execution.mode).toBe('REMOTE');
    expect(config.steps[0].execution.operatorId).toBe('charge-card');
    expect(config.steps[0].execution.protocol).toBe('PROTOBUF_HTTP_V1');
    expect(config.steps[0].execution.target.urlConfigKey).toBe('tpf.remote-operators.charge-card.url');
  });

  test('loadConfig rejects non-integer version values', () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'invalid-version.yaml');
    fs.writeFileSync(configPath, `version: "2"
appName: TestApp
basePackage: com.example.test
transport: GRPC
runtimeLayout: MODULAR
steps: []
`);

    expect(() => generator.loadConfig(configPath)).toThrow('Configuration version must be an integer');
  });

  test('toScaffoldConfig rejects missing top-level message definitions', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {},
      steps: [
        {
          name: 'Charge Card',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'ChargeRequest',
          outputTypeName: 'ChargeResult'
        }
      ]
    };

    expect(() => generator.toScaffoldConfig(config)).toThrow("Missing message definition for 'ChargeRequest'");
  });

  test('toScaffoldConfig rejects conflicting inline and top-level fields', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {
        ChargeRequest: {
          fields: [{ number: 1, name: 'orderId', type: 'uuid' }]
        },
        ChargeResult: {
          fields: [{ number: 1, name: 'paymentId', type: 'uuid' }]
        }
      },
      steps: [
        {
          name: 'Charge Card',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'ChargeRequest',
          inputFields: [{ number: 1, name: 'orderId', type: 'string' }],
          outputTypeName: 'ChargeResult'
        }
      ]
    };

    expect(() => generator.toScaffoldConfig(config)).toThrow('Conflicting inline vs top-level field definitions');
  });

  test('toScaffoldField preserves message references for repeated and map fields', () => {
    const generator = new PipelineGenerator();

    const repeatedMessage = generator.toScaffoldField({
      number: 1,
      name: 'customers',
      type: 'Customer',
      repeated: true
    });
    const mapWithMessageValue = generator.toScaffoldField({
      number: 2,
      name: 'customerById',
      type: 'map',
      keyType: 'string',
      valueType: 'Customer'
    });

    expect(repeatedMessage.type).toBe('List<Customer>');
    expect(repeatedMessage.protoType).toBe('Customer');
    expect(mapWithMessageValue.type).toBe('Map<String, Customer>');
    expect(mapWithMessageValue.protoType).toBe('map<string, Customer>');
  });
});
