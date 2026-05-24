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

  test('generateFromConfig emits parent pom aligned to root quarkus platform version', async () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'config.yaml');
    const outputPath = path.join(tempDir, 'generated-app');
    fs.writeFileSync(configPath, `version: 2
appName: Generated App
basePackage: com.example.generated
transport: GRPC
runtimeLayout: MODULAR
messages:
  Request:
    fields:
      - number: 1
        name: id
        type: uuid
  Response:
    fields:
      - number: 1
        name: status
        type: string
steps:
  - name: Process Request
    cardinality: ONE_TO_ONE
    inputTypeName: Request
    outputTypeName: Response
`);

    await generator.generateFromConfig(configPath, outputPath);

    const parentPom = fs.readFileSync(path.join(outputPath, 'pom.xml'), 'utf8');
    expect(parentPom).toContain('<quarkus.platform.version>3.33.1</quarkus.platform.version>');
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

    expect(step.inputTypeName).toBe('ChargeRequest');
    expect(step.outputTypeName).toBe('ChargeResult');
    expect(step.inputFields.map((field) => field.name)).toEqual(['orderId', 'amount']);
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
    expect(config.steps[0].execution.timeoutMs).toBe(3000);
    expect(config.steps[0].execution.target.urlConfigKey).toBe('tpf.remote-operators.charge-card.url');
  });

  test('loadConfig accepts await step metadata for v2 configs', () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'await-config.yaml');
    fs.writeFileSync(configPath, `version: 2
appName: TestApp
basePackage: com.example.test
transport: GRPC
runtimeLayout: MODULAR
messages:
  PaymentRecord:
    fields:
      - number: 1
        name: csvId
        type: string
  PaymentStatus:
    fields:
      - number: 1
        name: status
        type: string
steps:
  - name: Await Payment Provider
    kind: await
    cardinality: MANY_TO_MANY
    inputTypeName: PaymentRecord
    outputTypeName: PaymentStatus
    timeout: PT5M
    idempotencyKeyFields: [csvId]
    await:
      dispatch:
        mode: per-item
      correlation:
        strategy: signedResumeToken
      transport:
        type: kafka
        request:
          topic: csv-payments.payment.requests
          key: correlationId
        response:
          topic: csv-payments.payment.results
`);

    const config = generator.loadConfig(configPath);
    expect(config.steps[0].kind).toBe('await');
    expect(config.steps[0].await.transport.type).toBe('kafka');
    expect(config.steps[0].await.dispatch.mode).toBe('per-item');
  });

  test('toScaffoldConfig accepts interaction-api await steps without generating service modules', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {
        PaymentRecord: {
          fields: [{ number: 1, name: 'csvId', type: 'string' }]
        },
        PaymentStatus: {
          fields: [{ number: 1, name: 'status', type: 'string' }]
        }
      },
      steps: [
        {
          name: 'Await Payment Provider',
          kind: 'await',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'PaymentRecord',
          outputTypeName: 'PaymentStatus',
          timeout: 'PT5M',
          await: {
            correlation: { strategy: 'interactionId' },
            transport: { type: 'interaction-api' }
          }
        }
      ]
    };

    const scaffold = generator.toScaffoldConfig(config);
    expect(scaffold.steps[0].isAwaitStep).toBe(true);
    expect(scaffold.steps[0].awaitTransportType).toBe('interaction-api');
    expect(scaffold.steps[0].generatesServiceModule).toBe(false);
    expect(scaffold.steps[0].serviceName).toBe('await-payment-provider-svc');
  });

  test('toScaffoldConfig accepts webhook await steps without generating service modules', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {
        PaymentRecord: {
          fields: [{ number: 1, name: 'csvId', type: 'string' }]
        },
        PaymentStatus: {
          fields: [{ number: 1, name: 'status', type: 'string' }]
        }
      },
      steps: [
        {
          name: 'Await Payment Provider',
          kind: 'await',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'PaymentRecord',
          outputTypeName: 'PaymentStatus',
          timeout: 'PT5M',
          await: {
            correlation: { strategy: 'signedResumeToken' },
            transport: {
              type: 'webhook',
              request: { url: 'https://partner.example/payments' }
            }
          }
        }
      ]
    };

    const scaffold = generator.toScaffoldConfig(config);
    expect(scaffold.steps[0].isAwaitStep).toBe(true);
    expect(scaffold.steps[0].awaitTransportType).toBe('webhook');
    expect(scaffold.steps[0].generatesServiceModule).toBe(false);
  });

  test('toScaffoldConfig fails clearly for kafka await steps', () => {
    const generator = new PipelineGenerator();
    const config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      transport: 'GRPC',
      runtimeLayout: 'MODULAR',
      messages: {
        PaymentRecord: {
          fields: [{ number: 1, name: 'csvId', type: 'string' }]
        },
        PaymentStatus: {
          fields: [{ number: 1, name: 'status', type: 'string' }]
        }
      },
      steps: [
        {
          name: 'Await Payment Provider',
          kind: 'await',
          cardinality: 'MANY_TO_MANY',
          inputTypeName: 'PaymentRecord',
          outputTypeName: 'PaymentStatus',
          timeout: 'PT5M',
          await: {
            dispatch: { mode: 'per-item' },
            correlation: { strategy: 'signedResumeToken' },
            transport: {
              type: 'kafka',
              request: { topic: 'csv-payments.payment.requests', key: 'correlationId' },
              response: { topic: 'csv-payments.payment.results' }
            }
          }
        }
      ]
    };

    expect(() => generator.toScaffoldConfig(config)).toThrow(
      'The runtime supports Kafka await steps'
    );
  });

  test('generateFromConfig scaffolds interaction-api await guidance without creating a step module', async () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'interaction-await.yaml');
    const outputPath = path.join(tempDir, 'generated-app');
    fs.writeFileSync(configPath, `version: 2
appName: Await UI App
basePackage: com.example.awaitui
transport: GRPC
runtimeLayout: MODULAR
messages:
  ApprovalRequest:
    fields:
      - number: 1
        name: orderId
        type: uuid
  ApprovalDecision:
    fields:
      - number: 1
        name: status
        type: string
steps:
  - name: Await Approval
    kind: await
    cardinality: ONE_TO_ONE
    inputTypeName: ApprovalRequest
    outputTypeName: ApprovalDecision
    timeout: PT10M
    await:
      correlation:
        strategy: interactionId
      transport:
        type: interaction-api
`);

    await generator.generateFromConfig(configPath, outputPath);

    const readme = fs.readFileSync(path.join(outputPath, 'README.md'), 'utf8');
    expect(readme).toContain('generated await completion and pending-interaction query APIs');
    expect(readme).toContain('interaction-api await steps');
    expect(fs.existsSync(path.join(outputPath, 'await-approval-svc'))).toBe(false);
  });

  test('generateFromConfig scaffolds webhook await projects without creating a step module', async () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'webhook-await.yaml');
    const outputPath = path.join(tempDir, 'generated-app');
    fs.writeFileSync(configPath, `version: 2
appName: Webhook Await App
basePackage: com.example.awaitwebhook
transport: GRPC
runtimeLayout: MODULAR
messages:
  FraudCheckRequest:
    fields:
      - number: 1
        name: orderId
        type: uuid
  FraudCheckDecision:
    fields:
      - number: 1
        name: status
        type: string
steps:
  - name: Fraud Check
    kind: await
    cardinality: ONE_TO_ONE
    inputTypeName: FraudCheckRequest
    outputTypeName: FraudCheckDecision
    timeout: PT10M
    await:
      correlation:
        strategy: signedResumeToken
      transport:
        type: webhook
        request:
          url: https://partner.example/fraud-check
`);

    await generator.generateFromConfig(configPath, outputPath);

    const parentPom = fs.readFileSync(path.join(outputPath, 'pom.xml'), 'utf8');
    const readme = fs.readFileSync(path.join(outputPath, 'README.md'), 'utf8');
    const orchestratorProps = fs.readFileSync(
      path.join(outputPath, 'orchestrator-svc', 'src/main/resources', 'application.properties'),
      'utf8'
    );
    const runtimeMapping = YAML.load(
      fs.readFileSync(path.join(outputPath, 'config', 'runtime-mapping', 'modular-auto.yaml'), 'utf8')
    );

    expect(parentPom).not.toContain('<module>fraud-check-svc</module>');
    expect(fs.existsSync(path.join(outputPath, 'fraud-check-svc'))).toBe(false);
    expect(fs.existsSync(path.join(outputPath, 'config', 'pipeline.yaml'))).toBe(true);
    expect(readme).toContain('webhook await steps');
    expect(readme).toContain('pipeline.orchestrator.resume-token-secret');
    expect(orchestratorProps).toContain('pipeline.orchestrator.resume-token-secret=change-me');
    expect(runtimeMapping.modules['fraud-check-svc']).toBeUndefined();
    expect(runtimeMapping.modules['orchestrator-svc'].steps).toContain('fraud-check');
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

    expect(() => generator.loadConfig(configPath)).toThrow(
      'Configuration version must be a positive integer'
    );
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

  test('toScaffoldConfig accepts equivalent inline and top-level fields in different orders', () => {
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
            { number: 2, name: 'amount', type: 'decimal' },
            { number: 1, name: 'orderId', type: 'uuid' }
          ]
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
          inputFields: [
            { number: 1, name: 'orderId', type: 'uuid' },
            { number: 2, name: 'amount', type: 'decimal' }
          ],
          outputTypeName: 'ChargeResult'
        }
      ]
    };

    expect(() => generator.toScaffoldConfig(config)).not.toThrow();
  });

  test('loadConfig rejects LOCAL execution with remote-only fields', () => {
    const generator = new PipelineGenerator();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pipeline-generator-'));
    const configPath = path.join(tempDir, 'local-execution-config.yaml');
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
      mode: LOCAL
      operatorId: charge-card
`);

    expect(() => generator.loadConfig(configPath)).toThrow('Configuration validation failed');
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
