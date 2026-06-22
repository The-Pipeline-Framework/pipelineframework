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

package org.pipelineframework.processor.schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Exports the generator-facing pipeline template schema owned by deployment.
 *
 * <p>The schema is explicit and ordered on purpose: it documents the structural
 * contract consumed by external generators without reflecting over parser or
 * runtime model internals.</p>
 */
public final class PipelineTemplateSchemaExporter {
    public static final String RESOURCE_PATH = "META-INF/pipeline/pipeline-template-schema.json";

    private static final String SCHEMA_JSON = """
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pipelineframework.org/schemas/pipeline-template-config.schema.json",
  "title": "Pipeline Template Configuration",
  "description": "Generator-facing pipeline template configuration schema exported by framework/deployment.",
  "type": "object",
  "$defs": {
    "operatorReference": {
      "type": "string",
      "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*::[a-zA-Z_$][a-zA-Z\\\\d_$]*$"
    },
    "legacyFieldDefinition": {
      "type": "object",
      "properties": {
        "name": {
          "type": "string",
          "description": "Name of the field"
        },
        "type": {
          "type": "string",
          "description": "Java type of the field",
          "anyOf": [
            {
              "enum": [
                "String",
                "Integer",
                "Long",
                "Double",
                "Float",
                "Boolean",
                "UUID",
                "BigDecimal",
                "Currency",
                "Path",
                "LocalDateTime",
                "LocalDate",
                "OffsetDateTime",
                "ZonedDateTime",
                "Instant",
                "Duration",
                "Period",
                "URI",
                "URL",
                "File",
                "BigInteger",
                "AtomicInteger",
                "AtomicLong",
                "List<String>"
              ]
            },
            {
              "type": "string",
              "pattern": "^Map<[A-Z][A-Za-z0-9_]*, [A-Z][A-Za-z0-9_]*>$"
            },
            {
              "type": "string",
              "pattern": "^[A-Za-z][A-Za-z0-9_]*(\\\\.[A-Za-z][A-Za-z0-9_]*)*$"
            }
          ]
        },
        "protoType": {
          "type": "string",
          "description": "Protobuf type of the field",
          "anyOf": [
            {
              "enum": [
                "string",
                "int32",
                "int64",
                "double",
                "bool",
                "bytes",
                "float",
                "uint32",
                "uint64",
                "sint32",
                "sint64",
                "fixed32",
                "fixed64",
                "sfixed32",
                "sfixed64"
              ]
            },
            {
              "type": "string",
              "pattern": "^map<[a-z][a-z0-9_]*, [A-Za-z][A-Za-z0-9_]*(\\\\.[A-Za-z][A-Za-z0-9_]*)*>$"
            },
            {
              "type": "string",
              "pattern": "^[A-Za-z][A-Za-z0-9_]*(\\\\.[A-Za-z][A-Za-z0-9_]*)*$"
            }
          ]
        }
      },
      "required": [
        "name",
        "type",
        "protoType"
      ],
      "additionalProperties": false
    },
    "v2ProtoOverride": {
      "type": "object",
      "properties": {
        "encoding": {
          "type": "string",
          "enum": [
            "string",
            "bool",
            "int32",
            "int64",
            "float",
            "double",
            "bytes"
          ]
        }
      },
      "required": [
        "encoding"
      ],
      "additionalProperties": false
    },
    "v2FieldOverrides": {
      "type": "object",
      "properties": {
        "proto": {
          "$ref": "#/$defs/v2ProtoOverride"
        }
      },
      "additionalProperties": false
    },
    "v2FieldDefinition": {
      "type": "object",
      "properties": {
        "number": {
          "type": "integer",
          "minimum": 1
        },
        "name": {
          "type": "string"
        },
        "type": {
          "type": "string",
          "anyOf": [
            {
              "enum": [
                "string",
                "bool",
                "int32",
                "int64",
                "float32",
                "float64",
                "decimal",
                "uuid",
                "timestamp",
                "datetime",
                "date",
                "duration",
                "bytes",
                "currency",
                "uri",
                "path",
                "payload_ref",
                "map"
              ]
            },
            {
              "pattern": "^[A-Z][A-Za-z0-9_]*$"
            }
          ]
        },
        "keyType": {
          "type": "string",
          "enum": [
            "string",
            "bool",
            "int32",
            "int64"
          ]
        },
        "valueType": {
          "type": "string",
          "anyOf": [
            {
              "enum": [
                "string",
                "bool",
                "int32",
                "int64",
                "float32",
                "float64",
                "decimal",
                "uuid",
                "timestamp",
                "datetime",
                "date",
                "duration",
                "bytes",
                "currency",
                "uri",
                "path"
              ]
            },
            {
              "pattern": "^[A-Z][A-Za-z0-9_]*$"
            }
          ]
        },
        "optional": {
          "type": "boolean"
        },
        "repeated": {
          "type": "boolean"
        },
        "deprecated": {
          "type": "boolean"
        },
        "since": {
          "type": "string"
        },
        "deprecatedSince": {
          "type": "string"
        },
        "comment": {
          "type": "string"
        },
        "overrides": {
          "$ref": "#/$defs/v2FieldOverrides"
        },
        "referenceable": {
          "type": "object",
          "description": "Allows this scalar field to be represented out-of-line using a sibling payload_ref field.",
          "properties": {
            "refField": {
              "type": "string",
              "minLength": 1
            }
          },
          "required": [
            "refField"
          ],
          "additionalProperties": false
        }
      },
      "required": [
        "number",
        "name",
        "type"
      ],
      "allOf": [
        {
          "if": {
            "properties": {
              "type": {
                "const": "map"
              }
            },
            "required": [
              "type"
            ]
          },
          "then": {
            "required": [
              "keyType",
              "valueType"
            ]
          },
          "else": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "keyType"
                  ]
                },
                {
                  "required": [
                    "valueType"
                  ]
                }
              ]
            }
          }
        },
        {
          "not": {
            "properties": {
              "optional": {
                "const": true
              },
              "repeated": {
                "const": true
              }
            },
            "required": [
              "optional",
              "repeated"
            ]
          }
        }
      ],
      "additionalProperties": false
    },
    "v2Reserved": {
      "type": "object",
      "minProperties": 1,
      "properties": {
        "numbers": {
          "type": "array",
          "items": {
            "type": "integer",
            "minimum": 1
          }
        },
        "names": {
          "type": "array",
          "items": {
            "type": "string"
          }
        }
      },
      "additionalProperties": false
    },
    "v2MessageDefinition": {
      "type": "object",
      "properties": {
        "fields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "reserved": {
          "$ref": "#/$defs/v2Reserved"
        }
      },
      "required": [
        "fields"
      ],
      "additionalProperties": false
    },
    "v2RemoteTarget": {
      "type": "object",
      "properties": {
        "url": {
          "type": "string",
          "minLength": 1
        },
        "urlConfigKey": {
          "type": "string",
          "minLength": 1
        }
      },
      "oneOf": [
        {
          "required": [
            "url"
          ]
        },
        {
          "required": [
            "urlConfigKey"
          ]
        }
      ],
      "additionalProperties": false
    },
    "checkpointPublication": {
      "type": "object",
      "properties": {
        "publication": {
          "type": "string",
          "minLength": 1
        },
        "idempotencyKeyFields": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        }
      },
      "required": [
        "publication"
      ],
      "additionalProperties": false
    },
    "queryDefinition": {
      "type": "object",
      "properties": {
        "connector": {
          "const": "jpa"
        },
        "input": {
          "type": "string",
          "minLength": 1
        },
        "inputType": {
          "type": "string",
          "minLength": 1
        },
        "output": {
          "type": "string",
          "minLength": 1
        },
        "outputType": {
          "type": "string",
          "minLength": 1
        },
        "version": {
          "type": "string",
          "minLength": 1,
          "default": "v1"
        },
        "jpa": {
          "$ref": "#/$defs/jpaQueryDefinition"
        }
      },
      "required": [
        "connector",
        "jpa"
      ],
      "allOf": [
        {
          "anyOf": [
            {
              "required": [
                "input"
              ]
            },
            {
              "required": [
                "inputType"
              ]
            }
          ]
        },
        {
          "anyOf": [
            {
              "required": [
                "output"
              ]
            },
            {
              "required": [
                "outputType"
              ]
            }
          ]
        }
      ],
      "additionalProperties": false
    },
    "jpaQueryDefinition": {
      "type": "object",
      "properties": {
        "entity": {
          "type": "string",
          "minLength": 1
        },
        "where": {
          "type": "object",
          "minProperties": 1,
          "propertyNames": {
            "type": "string",
            "pattern": "^[A-Za-z_$][A-Za-z\\\\d_$]*(\\\\.[A-Za-z_$][A-Za-z\\\\d_$]*)*$"
          },
          "additionalProperties": {
            "oneOf": [
              {
                "type": "string",
                "minLength": 1
              },
              {
                "type": "object",
                "minProperties": 1,
                "maxProperties": 1,
                "properties": {
                  "eq": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "in": {
                    "oneOf": [
                      {
                        "$ref": "#/$defs/jpaPredicateScalar"
                      },
                      {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                          "$ref": "#/$defs/jpaPredicateScalar"
                        }
                      }
                    ]
                  },
                  "gt": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "gte": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "lt": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "lte": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "between": {
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 2,
                    "items": {
                      "$ref": "#/$defs/jpaPredicateScalar"
                    }
                  },
                  "like": {
                    "$ref": "#/$defs/jpaPredicateScalar"
                  },
                  "isNull": {
                    "oneOf": [
                      {
                        "type": "boolean"
                      },
                      {
                        "type": "string",
                        "pattern": "^([Tt][Rr][Uu][Ee]|[Ff][Aa][Ll][Ss][Ee])$"
                      }
                    ]
                  }
                },
                "additionalProperties": false
              }
            ]
          }
        },
        "projection": {
          "type": "object",
          "propertyNames": {
            "type": "string",
            "pattern": "^[A-Za-z_$][A-Za-z\\\\d_$]*(\\\\.[A-Za-z_$][A-Za-z\\\\d_$]*)*$"
          },
          "additionalProperties": {
            "type": "string",
            "pattern": "^[A-Za-z_$][A-Za-z\\\\d_$]*(\\\\.[A-Za-z_$][A-Za-z\\\\d_$]*)*$"
          }
        },
        "orderBy": {
          "type": "object",
          "minProperties": 1,
          "propertyNames": {
            "type": "string",
            "pattern": "^[A-Za-z_$][A-Za-z\\\\d_$]*(\\\\.[A-Za-z_$][A-Za-z\\\\d_$]*)*$"
          },
          "additionalProperties": {
            "type": "string",
            "pattern": "^([Aa][Ss][Cc]|[Dd][Ee][Ss][Cc])$"
          }
        },
        "limit": {
          "const": 1
        },
        "result": {
          "const": "single",
          "default": "single"
        }
      },
      "required": [
        "entity",
        "where"
      ],
      "allOf": [
        {
          "if": {
            "required": [
              "limit"
            ]
          },
          "then": {
            "required": [
              "orderBy"
            ]
          }
        }
      ],
      "additionalProperties": false
    },
    "jpaPredicateScalar": {
      "anyOf": [
        {
          "type": "string",
          "minLength": 1
        },
        {
          "type": "number"
        },
        {
          "type": "boolean"
        }
      ]
    },
    "queryCapture": {
      "type": "object",
      "properties": {
        "keyFields": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        }
      },
      "additionalProperties": false
    },
    "pipelineOutputBoundary": {
      "type": "object",
      "properties": {
        "checkpoint": {
          "$ref": "#/$defs/checkpointPublication"
        },
        "object": {
          "$ref": "#/$defs/objectOutputBoundary"
        },
        "to": {
          "type": "string",
          "minLength": 1
        },
        "consumes": {
          "$ref": "#/$defs/objectOutputConsume"
        }
      },
      "oneOf": [
        {
          "required": [
            "checkpoint"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "object"
                ]
              },
              {
                "required": [
                  "to"
                ]
              },
              {
                "required": [
                  "consumes"
                ]
              }
            ]
          }
        },
        {
          "required": [
            "object"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "checkpoint"
                ]
              },
              {
                "required": [
                  "to"
                ]
              },
              {
                "required": [
                  "consumes"
                ]
              }
            ]
          }
        },
        {
          "required": [
            "to",
            "consumes"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "checkpoint"
                ]
              },
              {
                "required": [
                  "object"
                ]
              }
            ]
          }
        }
      ],
      "additionalProperties": false
    },
    "checkpointSubscription": {
      "type": "object",
      "properties": {
        "publication": {
          "type": "string",
          "minLength": 1
        },
        "mapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        }
      },
      "required": [
        "publication"
      ],
      "additionalProperties": false
    },
    "objectInputEmit": {
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "minLength": 1
        },
        "typeName": {
          "type": "string",
          "minLength": 1
        },
        "mapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        }
      },
      "required": [
        "type",
        "mapper"
      ],
      "additionalProperties": false
    },
    "objectInputBoundary": {
      "type": "object",
      "properties": {
        "source": {
          "type": "string",
          "minLength": 1
        },
        "from": {
          "type": "string",
          "minLength": 1
        },
        "emits": {
          "$ref": "#/$defs/objectInputEmit"
        }
      },
      "required": [
        "emits"
      ],
      "oneOf": [
        {
          "required": [
            "source"
          ]
        },
        {
          "required": [
            "from"
          ]
        }
      ],
      "additionalProperties": false
    },
    "objectOutputConsume": {
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "minLength": 1
        },
        "typeName": {
          "type": "string",
          "minLength": 1
        },
        "mapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        }
      },
      "required": [
        "type",
        "mapper"
      ],
      "additionalProperties": false
    },
    "objectOutputBoundary": {
      "type": "object",
      "properties": {
        "target": {
          "type": "string",
          "minLength": 1
        },
        "to": {
          "type": "string",
          "minLength": 1
        },
        "consumes": {
          "$ref": "#/$defs/objectOutputConsume"
        }
      },
      "required": [
        "consumes"
      ],
      "oneOf": [
        {
          "required": [
            "target"
          ]
        },
        {
          "required": [
            "to"
          ]
        }
      ],
      "additionalProperties": false
    },
    "pipelineInputBoundary": {
      "type": "object",
      "properties": {
        "subscription": {
          "$ref": "#/$defs/checkpointSubscription"
        },
        "object": {
          "$ref": "#/$defs/objectInputBoundary"
        },
        "from": {
          "type": "string",
          "minLength": 1
        },
        "emits": {
          "$ref": "#/$defs/objectInputEmit"
        }
      },
      "oneOf": [
        {
          "required": [
            "subscription"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "object"
                ]
              },
              {
                "required": [
                  "from"
                ]
              },
              {
                "required": [
                  "emits"
                ]
              }
            ]
          }
        },
        {
          "required": [
            "object"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "subscription"
                ]
              },
              {
                "required": [
                  "from"
                ]
              },
              {
                "required": [
                  "emits"
                ]
              }
            ]
          }
        },
        {
          "required": [
            "from",
            "emits"
          ],
          "not": {
            "anyOf": [
              {
                "required": [
                  "subscription"
                ]
              },
              {
                "required": [
                  "object"
                ]
              }
            ]
          }
        }
      ],
      "additionalProperties": false
    },
    "objectSourceFilter": {
      "type": "object",
      "properties": {
        "include": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        },
        "exclude": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        }
      },
      "additionalProperties": false
    },
    "objectSourcePoll": {
      "type": "object",
      "properties": {
        "enabled": {
          "type": "boolean"
        },
        "interval": {
          "type": "string",
          "minLength": 1
        },
        "batchSize": {
          "type": "integer",
          "minimum": 1
        }
      },
      "additionalProperties": false
    },
    "objectSourceIdentity": {
      "type": "object",
      "properties": {
        "fields": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        }
      },
      "additionalProperties": false
    },
    "objectSourcePayload": {
      "type": "object",
      "properties": {
        "mode": {
          "type": "string",
          "enum": [
            "metadata",
            "reference",
            "text"
          ]
        },
        "refField": {
          "type": "string",
          "minLength": 1
        },
        "maxBytes": {
          "type": "integer",
          "minimum": 0
        },
        "charset": {
          "type": "string",
          "minLength": 1
        }
      },
      "additionalProperties": false
    },
    "objectSource": {
      "type": "object",
      "properties": {
        "kind": {
          "const": "object"
        },
        "provider": {
          "type": "string",
          "minLength": 1
        },
        "location": {
          "type": "object",
          "additionalProperties": true
        },
        "filter": {
          "$ref": "#/$defs/objectSourceFilter"
        },
        "poll": {
          "$ref": "#/$defs/objectSourcePoll"
        },
        "identity": {
          "$ref": "#/$defs/objectSourceIdentity"
        },
        "payload": {
          "$ref": "#/$defs/objectSourcePayload"
        }
      },
      "required": [
        "kind",
        "provider"
      ],
      "additionalProperties": false
    },
    "pipelineSources": {
      "type": "object",
      "additionalProperties": {
        "$ref": "#/$defs/objectSource"
      }
    },
    "objectPublishNaming": {
      "type": "object",
      "properties": {
        "keyTemplate": {
          "type": "string",
          "minLength": 1
        }
      },
      "additionalProperties": false
    },
    "objectPublishPayload": {
      "type": "object",
      "properties": {
        "contentType": {
          "type": "string",
          "minLength": 1
        },
        "charset": {
          "type": "string",
          "minLength": 1
        }
      },
      "additionalProperties": false
    },
    "objectPublishGrouping": {
      "type": "object",
      "properties": {
        "maxOpenGroups": {
          "type": "integer",
          "minimum": 1,
          "default": 32
        }
      },
      "additionalProperties": false
    },
    "objectPublishTarget": {
      "type": "object",
      "properties": {
        "kind": {
          "const": "object"
        },
        "provider": {
          "type": "string",
          "minLength": 1
        },
        "location": {
          "type": "object",
          "additionalProperties": true
        },
        "naming": {
          "$ref": "#/$defs/objectPublishNaming"
        },
        "payload": {
          "$ref": "#/$defs/objectPublishPayload"
        },
        "grouping": {
          "$ref": "#/$defs/objectPublishGrouping"
        }
      },
      "required": [
        "kind",
        "provider"
      ],
      "additionalProperties": false
    },
    "pipelinePublishTargets": {
      "type": "object",
      "additionalProperties": {
        "$ref": "#/$defs/objectPublishTarget"
      }
    },
    "v2Execution": {
      "type": "object",
      "required": [
        "mode"
      ],
      "properties": {
        "mode": {
          "type": "string",
          "enum": [
            "LOCAL",
            "REMOTE"
          ]
        },
        "operatorId": {
          "type": "string",
          "minLength": 1
        },
        "protocol": {
          "type": "string",
          "enum": [
            "PROTOBUF_HTTP_V1",
            "ENVELOPE_HTTP_V1"
          ]
        },
        "timeoutMs": {
          "type": "integer",
          "minimum": 1
        },
        "target": {
          "$ref": "#/$defs/v2RemoteTarget"
        }
      },
      "allOf": [
        {
          "if": {
            "properties": {
              "mode": {
                "enum": [
                  "REMOTE"
                ]
              }
            },
            "required": [
              "mode"
            ]
          },
          "then": {
            "required": [
              "operatorId",
              "protocol",
              "target"
            ]
          }
        },
        {
          "if": {
            "properties": {
              "mode": {
                "enum": [
                  "LOCAL"
                ]
              }
            },
            "required": [
              "mode"
            ]
          },
          "then": {
            "not": {
              "anyOf": [
                {
                  "required": [
                    "operatorId"
                  ]
                },
                {
                  "required": [
                    "protocol"
                  ]
                },
                {
                  "required": [
                    "target"
                  ]
                },
                {
                  "required": [
                    "timeoutMs"
                  ]
                }
              ]
            }
          }
        }
      ],
      "additionalProperties": false
    },
    "materializationAspect": {
      "type": "object",
      "description": "Framework-owned representation aspect for field-level reference/dereference materialization.",
      "properties": {
        "name": {
          "type": "string",
          "minLength": 1
        },
        "enabled": {
          "type": "boolean",
          "default": true
        },
        "scope": {
          "type": "string",
          "enum": [
            "GLOBAL",
            "STEPS"
          ],
          "default": "GLOBAL"
        },
        "position": {
          "type": "string",
          "enum": [
            "BEFORE_STEP",
            "AFTER_STEP"
          ],
          "default": "AFTER_STEP"
        },
        "order": {
          "type": "integer",
          "default": 0
        },
        "action": {
          "type": "string",
          "enum": [
            "reference",
            "dereference",
            "REFERENCE",
            "DEREFERENCE"
          ]
        },
        "message": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "fields": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "string",
            "minLength": 1
          }
        },
        "targetSteps": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        }
      },
      "required": [
        "name",
        "action",
        "message",
        "fields"
      ],
      "additionalProperties": false
    },
    "materialization": {
      "type": "object",
      "description": "Representation-aspect policies for transparent field materialization.",
      "properties": {
        "aspects": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/materializationAspect"
          }
        }
      },
      "additionalProperties": false
    },
    "legacyTemplateStep": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "name": {
          "type": "string"
        },
        "cardinality": {
          "type": "string",
          "enum": [
            "ONE_TO_ONE",
            "EXPANSION",
            "REDUCTION",
            "COLLAPSE",
            "SIDE_EFFECT",
            "MANY_TO_MANY",
            "ONE_TO_MANY",
            "MANY_TO_ONE"
          ]
        },
        "inputTypeName": {
          "type": "string"
        },
        "inputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/legacyFieldDefinition"
          }
        },
        "outputTypeName": {
          "type": "string"
        },
        "outputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/legacyFieldDefinition"
          }
        },
        "batchSize": {
          "type": "integer",
          "minimum": 1
        },
        "batchTimeoutMs": {
          "type": "integer",
          "minimum": 0
        },
        "parallel": {
          "type": "boolean"
        }
      },
      "required": [
        "name",
        "cardinality",
        "inputTypeName",
        "inputFields",
        "outputTypeName",
        "outputFields"
      ]
    },
    "v2TemplateStep": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "name": {
          "type": "string"
        },
        "cardinality": {
          "type": "string",
          "enum": [
            "ONE_TO_ONE",
            "EXPANSION",
            "REDUCTION",
            "COLLAPSE",
            "SIDE_EFFECT",
            "MANY_TO_MANY",
            "ONE_TO_MANY",
            "MANY_TO_ONE"
          ]
        },
        "inputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "inputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "outputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "outputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "batchSize": {
          "type": "integer",
          "minimum": 1
        },
        "batchTimeoutMs": {
          "type": "integer",
          "minimum": 0
        },
        "parallel": {
          "type": "boolean"
        },
        "execution": {
          "$ref": "#/$defs/v2Execution"
        },
        "id": {
          "type": "string",
          "minLength": 1
        },
        "kind": {
          "type": "string",
          "enum": [
            "internal",
            "delegated",
            "remote"
          ]
        },
        "flowRole": {
          "type": "string",
          "minLength": 1
        },
        "flowBoundaryRationale": {
          "type": "string"
        },
        "inboundMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "outboundMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        }
      },
      "allOf": [
        {
          "if": {
            "properties": {
              "execution": {
                "type": "object",
                "properties": {
                  "mode": {
                    "enum": [
                      "REMOTE"
                    ]
                  }
                },
                "required": [
                  "mode"
                ]
              }
            },
            "required": [
              "execution"
            ]
          },
          "then": {
            "properties": {
              "cardinality": {
                "enum": [
                  "ONE_TO_ONE"
                ]
              }
            }
          }
        }
      ],
      "required": [
        "name",
        "cardinality",
        "inputTypeName",
        "outputTypeName"
      ]
    },
    "delegatedOrInternalStep": {
      "type": "object",
      "properties": {
        "name": {
          "type": "string",
          "description": "Name of the step (e.g., Process Customer)"
        },
        "service": {
          "type": "string",
          "description": "Fully qualified class name of the YAML-declared internal service",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "operator": {
          "description": "Delegated operator reference in fully.qualified.Class::method format",
          "$ref": "#/$defs/operatorReference"
        },
        "delegate": {
          "description": "Legacy alias for operator (deprecated; prefer 'operator'): delegated operator reference in fully.qualified.Class::method format",
          "$ref": "#/$defs/operatorReference"
        },
        "input": {
          "type": "string",
          "description": "Fully qualified class name of the input type",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "output": {
          "type": "string",
          "description": "Fully qualified class name of the output type",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "operatorMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "externalMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "mapperFallback": {
          "type": "string",
          "enum": [
            "NONE",
            "JACKSON",
            "none",
            "jackson"
          ]
        },
        "runOnVirtualThreads": {
          "type": "boolean",
          "description": "Whether this YAML-declared internal blocking step should use virtual-thread offload."
        }
      },
      "oneOf": [
        {
          "required": [
            "name",
            "service"
          ]
        },
        {
          "required": [
            "name",
            "operator"
          ]
        },
        {
          "required": [
            "name",
            "delegate"
          ]
        }
      ],
      "allOf": [
        {
          "if": {
            "required": [
              "input"
            ]
          },
          "then": {
            "required": [
              "output"
            ]
          }
        },
        {
          "if": {
            "required": [
              "output"
            ]
          },
          "then": {
            "required": [
              "input"
            ]
          }
        },
        {
          "if": {
            "required": [
              "operator"
            ]
          },
          "then": {
            "not": {
              "required": [
                "runOnVirtualThreads"
              ]
            }
          }
        },
        {
          "if": {
            "required": [
              "delegate"
            ]
          },
          "then": {
            "not": {
              "required": [
                "runOnVirtualThreads"
              ]
            }
          }
        },
        {
          "not": {
            "required": [
              "operatorMapper",
              "externalMapper"
            ]
          }
        }
      ],
      "additionalProperties": false
    },
    "v2UnionVariant": {
      "type": "object",
      "properties": {
        "number": {
          "type": "integer",
          "minimum": 1
        },
        "type": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "name": {
          "type": "string",
          "pattern": "^[a-z][A-Za-z0-9_]*$"
        }
      },
      "required": [
        "number",
        "type"
      ],
      "additionalProperties": false
    },
    "v2UnionDefinition": {
      "type": "object",
      "properties": {
        "variants": {
          "type": "object",
          "propertyNames": {
            "type": "string",
            "pattern": "^[a-z][A-Za-z0-9_]*$"
          },
          "additionalProperties": {
            "$ref": "#/$defs/v2UnionVariant"
          },
          "minProperties": 1
        }
      },
      "required": [
        "variants"
      ],
      "additionalProperties": false
    },
    "awaitCorrelation": {
      "type": "object",
      "properties": {
        "strategy": {
          "type": "string",
          "minLength": 1
        }
      },
      "required": [
        "strategy"
      ],
      "additionalProperties": true
    },
    "awaitTransport": {
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "minLength": 1
        },
        "config": {
          "type": "object",
          "additionalProperties": true
        },
        "request": {
          "type": "object",
          "additionalProperties": true
        },
        "callback": {
          "type": "object",
          "additionalProperties": true
        },
        "response": {
          "type": "object",
          "additionalProperties": true
        },
        "consumer": {
          "type": "object",
          "additionalProperties": true
        },
        "headers": {
          "type": "object",
          "additionalProperties": true
        },
        "dispatch": {
          "type": "object",
          "additionalProperties": true
        },
        "url": {
          "type": "string",
          "minLength": 1
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": true
    },
    "awaitConfig": {
      "type": "object",
      "properties": {
        "correlation": {
          "$ref": "#/$defs/awaitCorrelation"
        },
        "transport": {
          "$ref": "#/$defs/awaitTransport"
        },
        "dispatch": {
          "type": "object",
          "additionalProperties": true
        }
      },
      "required": [
        "correlation",
        "transport"
      ],
      "additionalProperties": true
    },
    "commandTemplateStep": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "id": {
          "type": "string",
          "minLength": 1
        },
        "name": {
          "type": "string"
        },
        "kind": {
          "const": "command"
        },
        "command": {
          "type": "string",
          "minLength": 1
        },
        "cardinality": {
          "const": "ONE_TO_ONE"
        },
        "inputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "inputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "outputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "outputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "input": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "output": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "commandIdGenerator": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "duplicatePolicy": {
          "type": "string",
          "enum": [
            "RETURN_RECORDED",
            "FAIL",
            "return_recorded",
            "fail"
          ]
        },
        "config": {
          "type": "object",
          "additionalProperties": true
        },
        "flowRole": {
          "type": "string",
          "minLength": 1
        },
        "flowBoundaryRationale": {
          "type": "string"
        }
      },
      "allOf": [
        {
          "anyOf": [
            {
              "required": [
                "inputTypeName",
                "outputTypeName"
              ]
            },
            {
              "required": [
                "input",
                "output"
              ]
            }
          ]
        }
      ],
      "required": [
        "name",
        "kind",
        "command",
        "cardinality",
        "commandIdGenerator"
      ]
    },
    "awaitTemplateStep": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "id": {
          "type": "string",
          "minLength": 1
        },
        "name": {
          "type": "string"
        },
        "kind": {
          "const": "await"
        },
        "cardinality": {
          "type": "string",
          "enum": [
            "ONE_TO_ONE",
            "EXPANSION",
            "REDUCTION",
            "COLLAPSE",
            "SIDE_EFFECT",
            "MANY_TO_MANY",
            "ONE_TO_MANY",
            "MANY_TO_ONE"
          ]
        },
        "inputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "inputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "inboundMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "outputTypeName": {
          "type": "string",
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "outputFields": {
          "type": "array",
          "items": {
            "$ref": "#/$defs/v2FieldDefinition"
          }
        },
        "outboundMapper": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "input": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "output": {
          "type": "string",
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "timeout": {
          "type": "string",
          "minLength": 1
        },
        "idempotencyKeyFields": {
          "type": "array",
          "items": {
            "type": "string",
            "minLength": 1
          }
        },
        "await": {
          "$ref": "#/$defs/awaitConfig"
        },
        "batchSize": {
          "type": "integer",
          "minimum": 1
        },
        "batchTimeoutMs": {
          "type": "integer",
          "minimum": 0
        },
        "parallel": {
          "type": "boolean"
        },
        "flowRole": {
          "type": "string",
          "minLength": 1
        },
        "flowBoundaryRationale": {
          "type": "string"
        }
      },
      "allOf": [
        {
          "anyOf": [
            {
              "required": [
                "inputTypeName",
                "outputTypeName"
              ]
            },
            {
              "required": [
                "input",
                "output"
              ]
            }
          ]
        }
      ],
      "required": [
        "name",
        "kind",
        "cardinality",
        "timeout",
        "await"
      ]
    },
    "queryTemplateStep": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "id": {
          "type": "string",
          "minLength": 1
        },
        "name": {
          "type": "string"
        },
        "kind": {
          "const": "query"
        },
        "query": {
          "type": "string",
          "minLength": 1
        },
        "cardinality": {
          "type": "string",
          "enum": [
            "ONE_TO_ONE"
          ]
        },
        "inputTypeName": {
          "type": "string",
          "minLength": 1,
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "outputTypeName": {
          "type": "string",
          "minLength": 1,
          "pattern": "^[A-Z][A-Za-z0-9_]*$"
        },
        "input": {
          "type": "string",
          "minLength": 1,
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "output": {
          "type": "string",
          "minLength": 1,
          "pattern": "^[a-zA-Z_$][a-zA-Z\\\\d_$]*(\\\\.[a-zA-Z_$][a-zA-Z\\\\d_$]*)*\\\\.[A-Z][a-zA-Z\\\\d_$]*$"
        },
        "capture": {
          "$ref": "#/$defs/queryCapture"
        },
        "flowRole": {
          "type": "string",
          "minLength": 1
        },
        "flowBoundaryRationale": {
          "type": "string"
        }
      },
      "allOf": [
        {
          "anyOf": [
            {
              "required": [
                "inputTypeName",
                "outputTypeName"
              ]
            },
            {
              "required": [
                "input",
                "output"
              ]
            }
          ]
        }
      ],
      "required": [
        "name",
        "kind",
        "query",
        "cardinality"
      ]
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "version": {
            "const": 2
          }
        },
        "required": [
          "version"
        ]
      },
      "then": {
        "properties": {
          "steps": {
            "type": "array",
            "items": {
              "oneOf": [
                {
                  "$ref": "#/$defs/queryTemplateStep"
                },
                {
                  "$ref": "#/$defs/awaitTemplateStep"
                },
                {
                  "$ref": "#/$defs/commandTemplateStep"
                },
                {
                  "$ref": "#/$defs/v2TemplateStep"
                },
                {
                  "$ref": "#/$defs/delegatedOrInternalStep"
                }
              ]
            }
          }
        }
      },
      "else": {
        "not": {
          "required": [
            "messages"
          ]
        },
        "properties": {
          "steps": {
            "type": "array",
            "items": {
              "oneOf": [
                {
                  "$ref": "#/$defs/legacyTemplateStep"
                },
                {
                  "$ref": "#/$defs/delegatedOrInternalStep"
                }
              ]
            }
          }
        }
      }
    }
  ],
  "properties": {
    "version": {
      "type": "integer",
      "enum": [
        1,
        2
      ],
      "default": 1
    },
    "appName": {
      "type": "string",
      "description": "Name of the application to generate"
    },
    "basePackage": {
      "type": "string",
      "description": "Base package name for the generated Java code (e.g., com.example)",
      "pattern": "^[a-z][a-z0-9_]*(\\\\.[a-z0-9_]+)*$"
    },
    "transport": {
      "type": "string",
      "description": "Global transport for generated adapters",
      "enum": [
        "GRPC",
        "REST",
        "LOCAL"
      ],
      "default": "GRPC"
    },
    "platform": {
      "type": "string",
      "description": "Target deployment platform for generated build wiring",
      "enum": [
        "COMPUTE",
        "FUNCTION",
        "STANDARD",
        "LAMBDA",
        "compute",
        "function",
        "standard",
        "lambda"
      ],
      "default": "COMPUTE"
    },
    "runtimeLayout": {
      "type": "string",
      "description": "Target runtime layout intent for generated runtime-mapping files",
      "enum": [
        "MODULAR",
        "PIPELINE_RUNTIME",
        "MONOLITH",
        "modular",
        "pipeline-runtime",
        "monolith"
      ],
      "default": "MODULAR"
    },
    "messages": {
      "type": "object",
      "propertyNames": {
        "type": "string",
        "pattern": "^[A-Z][A-Za-z0-9_]*$"
      },
      "additionalProperties": {
        "$ref": "#/$defs/v2MessageDefinition"
      }
    },
    "steps": {
      "type": "array",
      "description": "List of pipeline steps."
    },
    "queries": {
      "type": "object",
      "description": "Captured query connector definitions referenced by kind: query steps.",
      "propertyNames": {
        "type": "string",
        "minLength": 1
      },
      "additionalProperties": {
        "$ref": "#/$defs/queryDefinition"
      }
    },
    "aspects": {
      "type": "object",
      "description": "Pipeline aspects - cross-cutting concerns that apply around pipeline steps. Aspect names must be lower-kebab-case and typically match the plugin module base name (e.g., persistence -> persistence-svc). Cache invalidation aspects (cache-invalidate, cache-invalidate-all) are hosted in cache-invalidation-svc.",
      "propertyNames": {
        "type": "string",
        "pattern": "^[a-z][a-z0-9-]*$"
      },
      "additionalProperties": {
        "type": "object",
        "properties": {
          "enabled": {
            "type": "boolean",
            "description": "Whether the aspect is enabled",
            "default": true
          },
          "scope": {
            "type": "string",
            "description": "Scope of the aspect application",
            "enum": [
              "GLOBAL",
              "STEPS"
            ]
          },
          "position": {
            "type": "string",
            "description": "Position relative to the step",
            "enum": [
              "BEFORE_STEP",
              "AFTER_STEP"
            ]
          },
          "order": {
            "type": "integer",
            "description": "Order of application (lower executes closer to the step boundary). Negative values are allowed and execute before zero-valued aspects.",
            "default": 0
          },
          "config": {
            "type": "object",
            "description": "Free-form configuration for the aspect",
            "additionalProperties": true
          }
        },
        "required": [
          "enabled",
          "scope",
          "position"
        ],
        "additionalProperties": false
      }
    },
    "materialization": {
      "$ref": "#/$defs/materialization"
    },
    "sources": {
      "$ref": "#/$defs/pipelineSources"
    },
    "publish": {
      "$ref": "#/$defs/pipelinePublishTargets"
    },
    "input": {
      "$ref": "#/$defs/pipelineInputBoundary"
    },
    "output": {
      "$ref": "#/$defs/pipelineOutputBoundary"
    },
    "unions": {
      "type": "object",
      "propertyNames": {
        "type": "string",
        "pattern": "^[A-Z][A-Za-z0-9_]*$"
      },
      "additionalProperties": {
        "$ref": "#/$defs/v2UnionDefinition"
      }
    }
  },
  "required": [
    "appName",
    "basePackage",
    "steps"
  ]
}
""";

    private PipelineTemplateSchemaExporter() {
    }

    public static String schemaJson() {
        return SCHEMA_JSON;
    }

    public static void writeTo(Path output) throws IOException {
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(output, SCHEMA_JSON, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.print(SCHEMA_JSON);
            return;
        }
        writeTo(Path.of(args[0]));
    }
}
