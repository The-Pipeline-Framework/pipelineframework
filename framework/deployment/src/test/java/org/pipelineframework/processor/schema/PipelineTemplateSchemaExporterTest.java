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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTemplateSchemaExporterTest {

    @Test
    void packagedSchemaMatchesFreshExporterOutput() throws IOException {
        String packaged = readPackagedSchema();

        assertEquals(PipelineTemplateSchemaExporter.schemaJson(), packaged);
    }

    @Test
    void exportsDraft202012SchemaWithGeneratorFacingDefinitions() {
        JsonObject schema = parse(PipelineTemplateSchemaExporter.schemaJson());

        assertEquals("https://json-schema.org/draft/2020-12/schema", text(schema, "$schema"));
        assertEquals("Pipeline Template Configuration", text(schema, "title"));
        assertTrue(schema.has("$defs"));
        assertTrue(schema.has("properties"));

        JsonObject definitions = schema.getAsJsonObject("$defs");
        assertTrue(definitions.has("v2MessageDefinition"));
        assertTrue(definitions.has("v2UnionDefinition"));
        assertTrue(definitions.has("pipelineInputBoundary"));
        assertTrue(definitions.has("pipelineOutputBoundary"));
        assertTrue(definitions.has("pipelineSources"));
        assertTrue(definitions.has("pipelinePublishTargets"));
        assertTrue(definitions.has("objectSource"));
        assertTrue(definitions.has("queryDefinition"));
        assertTrue(definitions.has("jpaQueryDefinition"));
        assertTrue(definitions.has("queryCapture"));
        assertTrue(definitions.has("queryTemplateStep"));
        assertTrue(definitions.has("objectPublishTarget"));
        assertTrue(definitions.has("delegatedOrInternalStep"));
        assertTrue(definitions.has("v2Execution"));
        assertTrue(definitions.has("awaitTemplateStep"));
        assertTrue(definitions.has("materialization"));
        assertTrue(definitions.has("materializationAspect"));

        JsonObject properties = schema.getAsJsonObject("properties");
        assertTrue(properties.has("types"));
        assertTrue(properties.has("messages"));
        assertTrue(properties.getAsJsonObject("messages").get("deprecated").getAsBoolean());
        assertTrue(properties.has("unions"));
        assertTrue(properties.has("input"));
        assertTrue(properties.has("output"));
        assertTrue(properties.has("sources"));
        assertTrue(properties.has("queries"));
        assertTrue(properties.has("publish"));
        assertTrue(properties.has("materialization"));
    }

    @Test
    void compactTypesAndLogicalContractsAreRepresentedWithoutChangingPhysicalBoundaryShapes() {
        JsonObject schema = parse(PipelineTemplateSchemaExporter.schemaJson());
        JsonObject definitions = schema.getAsJsonObject("$defs");
        JsonObject fieldDefinition = definitions.getAsJsonObject("v2FieldDefinition");
        JsonObject tuple = definitions.getAsJsonObject("v2FieldTupleDefinition");

        assertEquals(2, fieldDefinition.getAsJsonArray("oneOf").size());
        assertEquals(3, tuple.getAsJsonArray("prefixItems").size());
        assertEquals(3, tuple.get("minItems").getAsInt());
        assertEquals(3, tuple.get("maxItems").getAsInt());
        assertFalse(tuple.get("items").getAsBoolean());

        JsonObject properties = schema.getAsJsonObject("properties");
        assertEquals("#/$defs/pipelineInputBoundary", properties.getAsJsonObject("input").get("$ref").getAsString());
        assertEquals("#/$defs/pipelineOutputBoundary", properties.getAsJsonObject("output").get("$ref").getAsString());
        JsonObject contract = properties.getAsJsonObject("contract");
        assertTrue(contract.has("properties"));
        assertTrue(contract.getAsJsonObject("properties").has("input"));
        assertTrue(contract.getAsJsonObject("properties").has("output"));
        assertFalse(contract.get("additionalProperties").getAsBoolean());

        JsonObject aliasExclusion = schema.getAsJsonArray("allOf").get(1).getAsJsonObject()
            .getAsJsonObject("not");
        assertContains(aliasExclusion.getAsJsonArray("required"), "types");
        assertContains(aliasExclusion.getAsJsonArray("required"), "messages");
    }

    @Test
    void queryStepSchemaRequiresQueryReferenceAndCaptureFields() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject queryDefinition = definitions.getAsJsonObject("queryDefinition");
        JsonObject queryProperties = queryDefinition.getAsJsonObject("properties");
        assertEquals("jpa", queryProperties.getAsJsonObject("connector").get("const").getAsString());
        assertContains(queryDefinition.getAsJsonArray("required"), "jpa");

        JsonObject jpaDefinition = definitions.getAsJsonObject("jpaQueryDefinition");
        assertContains(jpaDefinition.getAsJsonArray("required"), "entity");
        assertContains(jpaDefinition.getAsJsonArray("required"), "where");
        JsonObject jpaProperties = jpaDefinition.getAsJsonObject("properties");
        assertTrue(jpaProperties.has("orderBy"));
        assertTrue(jpaProperties.has("limit"));
        JsonObject whereDefinition = jpaProperties.getAsJsonObject("where");
        JsonArray whereShapes = whereDefinition.getAsJsonObject("additionalProperties").getAsJsonArray("oneOf");
        assertEquals(2, whereShapes.size());
        JsonObject predicateObject = whereShapes.get(1).getAsJsonObject();
        JsonObject predicateProperties = predicateObject.getAsJsonObject("properties");
        assertTrue(predicateProperties.has("eq"));
        assertTrue(predicateProperties.has("gte"));
        assertTrue(predicateProperties.has("between"));
        assertTrue(predicateProperties.has("isNull"));
        assertTrue(predicateProperties.getAsJsonObject("isNull").has("oneOf"));
        JsonObject orderByDefinition = jpaProperties.getAsJsonObject("orderBy");
        assertEquals(1, orderByDefinition.get("minProperties").getAsInt());
        assertTrue(orderByDefinition.getAsJsonObject("additionalProperties").has("pattern"));
        assertTrue(jpaDefinition.has("allOf"));
        assertTrue(definitions.has("jpaPredicateScalar"));
        assertTrue(definitions.getAsJsonObject("jpaPredicateScalar").has("anyOf"));

        JsonObject queryStep = definitions.getAsJsonObject("queryTemplateStep");

        JsonObject properties = queryStep.getAsJsonObject("properties");
        assertTrue(properties.has("query"));
        assertTrue(properties.has("capture"));
        assertContains(queryStep.getAsJsonArray("required"), "query");
    }

    @Test
    void objectInputBoundaryRequiresSourceReference() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject objectInput = definitions.getAsJsonObject("objectInputBoundary");

        assertContains(objectInput.getAsJsonArray("required"), "emits");
        JsonArray oneOf = objectInput.getAsJsonArray("oneOf");
        assertEquals(2, oneOf.size());
        assertContains(oneOf.get(0).getAsJsonObject().getAsJsonArray("required"), "source");
        assertContains(oneOf.get(1).getAsJsonObject().getAsJsonArray("required"), "from");
    }

    @Test
    void objectOutputBoundaryRequiresTargetReference() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject objectOutput = definitions.getAsJsonObject("objectOutputBoundary");

        assertContains(objectOutput.getAsJsonArray("required"), "consumes");
        JsonArray oneOf = objectOutput.getAsJsonArray("oneOf");
        assertEquals(2, oneOf.size());
        assertContains(oneOf.get(0).getAsJsonObject().getAsJsonArray("required"), "target");
        assertContains(oneOf.get(1).getAsJsonObject().getAsJsonArray("required"), "to");
    }

    @Test
    void materializationSchemaIncludesReferenceablePayloadRefSurface() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");

        JsonObject fieldDefinition = definitions.getAsJsonObject("v2FieldObjectDefinition");
        JsonObject fieldProperties = fieldDefinition.getAsJsonObject("properties");
        assertTrue(fieldProperties.has("referenceable"));

        JsonArray typeEnums = fieldProperties
            .getAsJsonObject("type")
            .getAsJsonArray("anyOf")
            .get(0)
            .getAsJsonObject()
            .getAsJsonArray("enum");
        assertContains(typeEnums, "payload_ref");

        JsonObject referenceable = fieldProperties.getAsJsonObject("referenceable");
        assertContains(referenceable.getAsJsonArray("required"), "refField");

        JsonObject materializationAspect = definitions.getAsJsonObject("materializationAspect");
        JsonObject materializationProperties = materializationAspect.getAsJsonObject("properties");
        assertTrue(materializationProperties.has("action"));
        assertTrue(materializationProperties.has("message"));
        assertTrue(materializationProperties.has("fields"));
        assertTrue(materializationProperties.has("targetSteps"));
        assertContains(materializationAspect.getAsJsonArray("required"), "action");
        assertContains(materializationAspect.getAsJsonArray("required"), "message");
        assertContains(materializationAspect.getAsJsonArray("required"), "fields");
    }

    @Test
    void awaitStepShapeIncludesStructuralRuntimeContract() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject awaitStep = definitions.getAsJsonObject("awaitTemplateStep");
        JsonObject awaitProperties = awaitStep.getAsJsonObject("properties");

        assertEquals("await", awaitProperties.getAsJsonObject("kind").get("const").getAsString());
        assertContains(awaitStep.getAsJsonArray("required"), "kind");
        assertContains(awaitStep.getAsJsonArray("required"), "timeout");
        assertContains(awaitStep.getAsJsonArray("required"), "await");
        assertTrue(awaitProperties.has("idempotencyKeyFields"));

        JsonObject awaitConfig = definitions.getAsJsonObject("awaitConfig");
        assertContains(awaitConfig.getAsJsonArray("required"), "correlation");
        assertContains(awaitConfig.getAsJsonArray("required"), "transport");
        assertTrue(awaitConfig.getAsJsonObject("properties").has("dispatch"),
            "schema stays structural; parser owns the semantic await.dispatch rejection");

        JsonObject correlation = definitions.getAsJsonObject("awaitCorrelation");
        assertContains(correlation.getAsJsonArray("required"), "strategy");

        JsonObject transport = definitions.getAsJsonObject("awaitTransport");
        assertContains(transport.getAsJsonArray("required"), "type");
        assertTrue(transport.getAsJsonObject("properties").has("config"));
        assertTrue(transport.getAsJsonObject("properties").has("request"));
        assertTrue(transport.getAsJsonObject("properties").has("callback"));
    }

    @Test
    void remoteExecutionProtocolEnumIncludesEnvelopeCompatibilityProtocol() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject execution = definitions.getAsJsonObject("v2Execution");
        JsonArray protocols = execution.getAsJsonObject("properties")
            .getAsJsonObject("protocol")
            .getAsJsonArray("enum");

        assertContains(protocols, "PROTOBUF_HTTP_V1");
        assertContains(protocols, "ENVELOPE_HTTP_V1");
    }

    @Test
    void internalStepShapeIncludesVirtualThreadHint() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");
        JsonObject step = definitions.getAsJsonObject("delegatedOrInternalStep");
        JsonObject properties = step.getAsJsonObject("properties");

        JsonObject runOnVirtualThreads = properties.getAsJsonObject("runOnVirtualThreads");
        assertNotNull(runOnVirtualThreads);
        assertEquals("boolean", text(runOnVirtualThreads, "type"));
        JsonArray allOf = step.getAsJsonArray("allOf");
        assertTrue(allOf.toString().contains("\"operator\""));
        assertTrue(allOf.toString().contains("\"delegate\""));
        assertTrue(allOf.toString().contains("\"runOnVirtualThreads\""));
    }

    @Test
    void branchAwareStepShapesExposeAcceptsAndTerminal() {
        JsonObject definitions = parse(PipelineTemplateSchemaExporter.schemaJson()).getAsJsonObject("$defs");

        JsonObject internalStep = definitions.getAsJsonObject("delegatedOrInternalStep");
        JsonObject internalProperties = internalStep.getAsJsonObject("properties");
        assertTrue(internalProperties.has("accepts"));
        assertTrue(internalProperties.has("terminal"));
        assertEquals("boolean", internalProperties.getAsJsonObject("terminal").get("type").getAsString());

        JsonObject accepts = internalProperties.getAsJsonObject("accepts");
        assertEquals("array", accepts.get("type").getAsString());
        assertEquals("^[A-Z][A-Za-z0-9_]*$", accepts.getAsJsonObject("items").get("pattern").getAsString());

        assertTrue(definitions.getAsJsonObject("awaitTemplateStep").getAsJsonObject("properties").has("accepts"));
        assertTrue(definitions.getAsJsonObject("commandTemplateStep").getAsJsonObject("properties").has("terminal"));
        assertTrue(definitions.getAsJsonObject("queryTemplateStep").getAsJsonObject("properties").has("accepts"));
    }

    @Test
    void deterministicTopLevelOrderingKeepsSchemaStableForConsumers() {
        JsonObject schema = parse(PipelineTemplateSchemaExporter.schemaJson());
        List<String> keys = new ArrayList<>();
        schema.keySet().forEach(keys::add);

        assertEquals(List.of(
            "$schema",
            "$id",
            "title",
            "description",
            "type",
            "$defs",
            "allOf",
            "properties",
            "required"
        ), keys);
    }

    private static String readPackagedSchema() throws IOException {
        try (InputStream input = PipelineTemplateSchemaExporterTest.class.getClassLoader()
            .getResourceAsStream(PipelineTemplateSchemaExporter.RESOURCE_PATH)) {
            assertNotNull(input, "packaged pipeline template schema resource is missing");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JsonObject parse(String json) {
        return JsonParser.parseString(json).getAsJsonObject();
    }

    private static String text(JsonObject object, String property) {
        return object.get(property).getAsString();
    }

    private static void assertContains(JsonArray array, String expected) {
        for (JsonElement element : array) {
            if (expected.equals(element.getAsString())) {
                return;
            }
        }
        throw new AssertionError("Expected " + array + " to contain " + expected);
    }
}
