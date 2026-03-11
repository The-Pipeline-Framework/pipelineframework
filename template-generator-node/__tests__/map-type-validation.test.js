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
const path = require('path');
const Ajv = require('ajv');
const schema = require('../src/pipeline-template-schema.json');

const ajv = new Ajv();

describe('Map Type Validation', () => {
  let validate;

  beforeAll(() => {
    validate = ajv.compile(schema);
  });

  test('valid Map types should pass validation', () => {
    // Valid inputFields Map types
    const validInputConfig = {
      "appName": "TestApp",
      "basePackage": "com.example.test",
      "steps": [
        {
          "name": "Test Step",
          "cardinality": "ONE_TO_ONE",
          "inputTypeName": "TestInput",
          "inputFields": [
            {
              "name": "mapField",
              "type": "Map<String, Integer>",
              "protoType": "map<string, int32>"
            }
          ],
          "outputTypeName": "TestOutput",
          "outputFields": [
            {
              "name": "outputMapField",
              "type": "Map<String, Integer>", 
              "protoType": "map<string, int32>"
            }
          ]
        }
      ]
    };
    
    expect(validate(validInputConfig)).toBe(true);
  });

  test('valid complex Map types should pass validation', () => {
    const complexMapConfig = {
      "appName": "TestApp",
      "basePackage": "com.example.test",
      "steps": [
        {
          "name": "Test Step",
          "cardinality": "ONE_TO_ONE",
          "inputTypeName": "TestInput",
          "inputFields": [
            {
              "name": "mapField1",
              "type": "Map<CustomClass, AnotherClass>",
              "protoType": "map<string, string>"
            },
            {
              "name": "mapField2", 
              "type": "Map<String, Long>",
              "protoType": "map<string, int64>"
            }
          ],
          "outputTypeName": "TestOutput",
          "outputFields": [
            {
              "name": "outputMapField1",
              "type": "Map<CustomOutput, AnotherOutput>",
              "protoType": "map<string, string>"
            },
            {
              "name": "outputMapField2",
              "type": "Map<LocalDate, UUID>",
              "protoType": "map<string, string>"
            }
          ]
        }
      ]
    };
    
    expect(validate(complexMapConfig)).toBe(true);
  });

  test('valid v2 semantic map fields should pass validation', () => {
    const v2Config = {
      "version": 2,
      "appName": "TestApp",
      "basePackage": "com.example.test",
      "messages": {
        "TestInput": {
          "fields": [
            {
              "number": 1,
              "name": "attributes",
              "type": "map",
              "keyType": "string",
              "valueType": "int32"
            }
          ]
        },
        "TestOutput": {
          "fields": [
            {
              "number": 1,
              "name": "status",
              "type": "string"
            }
          ],
          "reserved": {
            "numbers": [4],
            "names": ["legacyField"]
          }
        }
      },
      "steps": [
        {
          "name": "Test Step",
          "cardinality": "ONE_TO_ONE",
          "inputTypeName": "TestInput",
          "outputTypeName": "TestOutput"
        }
      ]
    };

    expect(validate(v2Config)).toBe(true);
  });

  test('invalid v2 semantic map keyType/valueType should fail', () => {
    const v2Config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      messages: {
        TestInput: {
          fields: [
            {
              number: 1,
              name: 'attributes',
              type: 'map',
              keyType: 'float32',
              valueType: 'flot32'
            }
          ]
        }
      },
      steps: [
        {
          name: 'Test Step',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'TestInput',
          outputTypeName: 'TestInput'
        }
      ]
    };

    expect(validate(v2Config)).toBe(false);
  });

  test('missing map keyType/valueType should fail', () => {
    const v2Config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      messages: {
        TestInput: {
          fields: [
            {
              number: 1,
              name: 'attributes',
              type: 'map'
            }
          ]
        }
      },
      steps: [
        {
          name: 'Test Step',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'TestInput',
          outputTypeName: 'TestInput'
        }
      ]
    };

    expect(validate(v2Config)).toBe(false);
  });

  test('invalid reserved config should fail', () => {
    const v2Config = {
      version: 2,
      appName: 'TestApp',
      basePackage: 'com.example.test',
      messages: {
        TestInput: {
          fields: [
            {
              number: 1,
              name: 'status',
              type: 'string'
            }
          ],
          reserved: {
            numbers: ['four'],
            names: [1]
          }
        }
      },
      steps: [
        {
          name: 'Test Step',
          cardinality: 'ONE_TO_ONE',
          inputTypeName: 'TestInput',
          outputTypeName: 'TestInput'
        }
      ]
    };

    expect(validate(v2Config)).toBe(false);
  });

  test('invalid Map types should fail validation', () => {
    // Invalid Map types that should fail
    const invalidConfigs = [
      // inputFields with invalid Map types
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [
              {
                "name": "badMapField",
                "type": "Map<lowercaseClass, String>", // invalid - lowercase class name
                "protoType": "map<string, int32>"
              }
            ],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "outputMapField",
                "type": "Map<String, Integer>",
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      },
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [
              {
                "name": "mapField",
                "type": "Map<String, Integer>",
                "protoType": "map<string, int32>"
              }
            ],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "badOutputMapField",
                "type": "Map<lowercaseClass, String>", // invalid - lowercase class name
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      },
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [
              {
                "name": "mapField", 
                "type": "Map<String, Integer>",
                "protoType": "map<string, int32>"
              }
            ],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "badOutputMapField",
                "type": "Map<$Invalid, Class>", // invalid character
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      },
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [
              {
                "name": "badProtoMapField",
                "type": "Map<String, Integer>",
                "protoType": "map<InvalidUppercase, int32>" // invalid proto type (starts with uppercase)
              }
            ],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "outputMapField",
                "type": "Map<String, Integer>",
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      }
    ];

    invalidConfigs.forEach((config) => {
      expect(validate(config)).toBe(false);
      expect(validate.errors).toBeDefined();
    });
  });

  test('permissive Map patterns should no longer match', () => {
    // These used to match the old permissive pattern but should now fail
    const configsThatShouldFail = [
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "badMapField",
                "type": "Map<lowercase, Uppercase>", // would have matched old pattern
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      },
      {
        "appName": "TestApp",
        "basePackage": "com.example.test",
        "steps": [
          {
            "name": "Test Step",
            "cardinality": "ONE_TO_ONE",
            "inputTypeName": "TestInput",
            "inputFields": [],
            "outputTypeName": "TestOutput",
            "outputFields": [
              {
                "name": "badMapField",
                "type": "Map<123numeric, ValidClass>", // would have matched old pattern
                "protoType": "map<string, int32>"
              }
            ]
          }
        ]
      }
    ];

    configsThatShouldFail.forEach((config) => {
      expect(validate(config)).toBe(false);
      expect(validate.errors).toBeDefined();
    });
  });
});
