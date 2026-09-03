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

package org.pipelineframework.openapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.openapi.OASFactory;
import org.eclipse.microprofile.openapi.models.Components;
import org.eclipse.microprofile.openapi.models.OpenAPI;
import org.eclipse.microprofile.openapi.models.Operation;
import org.eclipse.microprofile.openapi.models.PathItem;
import org.eclipse.microprofile.openapi.models.Paths;
import org.eclipse.microprofile.openapi.models.media.Content;
import org.eclipse.microprofile.openapi.models.media.MediaType;
import org.eclipse.microprofile.openapi.models.media.Schema;
import org.eclipse.microprofile.openapi.models.parameters.RequestBody;
import org.junit.jupiter.api.Test;

class PublicApiContractFilterTest {

    @Test
    void retainsConfiguredPathsAndTheirTransitiveSchemaClosure() {
        Schema caseRequest = OASFactory.createSchema()
            .addProperty("operation", reference("ProposedOperation"));
        Schema proposedOperation = OASFactory.createSchema()
            .oneOf(List.of(reference("SetField"), reference("AddMember")))
            .discriminator(OASFactory.createDiscriminator()
                .propertyName("operation")
                .mapping(Map.of("mapped", "#/components/schemas/MappedVariant")));
        Schema setField = OASFactory.createSchema().addProperty("path", OASFactory.createSchema());
        Schema addMember = OASFactory.createSchema().addProperty("relationship", OASFactory.createSchema());
        Schema mappedVariant = OASFactory.createSchema().addProperty("value", OASFactory.createSchema());
        Schema hostedRequest = OASFactory.createSchema().addProperty("payload", reference("JsonNode"));

        Components components = OASFactory.createComponents()
            .addSchema("CaseRequest", caseRequest)
            .addSchema("ProposedOperation", proposedOperation)
            .addSchema("SetField", setField)
            .addSchema("AddMember", addMember)
            .addSchema("MappedVariant", mappedVariant)
            .addSchema("HostedRequest", hostedRequest)
            .addSchema("JsonNode", OASFactory.createSchema());
        Paths paths = OASFactory.createPaths()
            .addPathItem("/api/cases/{caseId}/operations", post(referenceBody("CaseRequest")))
            .addPathItem("/q/pipeline/process", post(referenceBody("HostedRequest")));
        OpenAPI openApi = OASFactory.createOpenAPI().components(components).paths(paths);

        new PublicApiContractFilter(List.of("/api/cases/")).filterOpenAPI(openApi);

        assertEquals(Set.of("/api/cases/{caseId}/operations"), openApi.getPaths().getPathItems().keySet());
        assertEquals(
            Set.of("CaseRequest", "ProposedOperation", "SetField", "AddMember", "MappedVariant"),
            openApi.getComponents().getSchemas().keySet());
    }

    @Test
    void rejectsMissingOrInvalidPublicRoots() {
        assertThrows(IllegalStateException.class, () -> new PublicApiContractFilter(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new PublicApiContractFilter(List.of("api/cases")));
    }

    private PathItem post(RequestBody body) {
        Operation operation = OASFactory.createOperation().requestBody(body);
        return OASFactory.createPathItem().POST(operation);
    }

    private RequestBody referenceBody(String schemaName) {
        Content content = OASFactory.createContent()
            .addMediaType("application/json", OASFactory.createMediaType().schema(reference(schemaName)));
        return OASFactory.createRequestBody().content(content);
    }

    private Schema reference(String name) {
        return OASFactory.createSchema().ref("#/components/schemas/" + name);
    }
}
