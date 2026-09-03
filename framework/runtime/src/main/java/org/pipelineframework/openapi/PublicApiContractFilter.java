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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.openapi.OASFilter;
import org.eclipse.microprofile.openapi.models.Reference;
import org.eclipse.microprofile.openapi.models.Components;
import org.eclipse.microprofile.openapi.models.OpenAPI;
import org.eclipse.microprofile.openapi.models.Operation;
import org.eclipse.microprofile.openapi.models.PathItem;
import org.eclipse.microprofile.openapi.models.callbacks.Callback;
import org.eclipse.microprofile.openapi.models.headers.Header;
import org.eclipse.microprofile.openapi.models.media.Content;
import org.eclipse.microprofile.openapi.models.media.Discriminator;
import org.eclipse.microprofile.openapi.models.media.Schema;
import org.eclipse.microprofile.openapi.models.parameters.Parameter;
import org.eclipse.microprofile.openapi.models.parameters.RequestBody;
import org.eclipse.microprofile.openapi.models.responses.APIResponse;

/**
 * Restricts a generated OpenAPI document to configured application paths and their schema closure.
 *
 * <p>Activate this filter with {@code mp.openapi.filter} and configure one or more path roots with
 * {@value #PUBLIC_PATH_PREFIXES}. The filter fails closed when no roots are configured.</p>
 */
public final class PublicApiContractFilter implements OASFilter {

    public static final String PUBLIC_PATH_PREFIXES = "pipeline.openapi.public-path-prefixes";
    private static final String SCHEMA_REFERENCE_PREFIX = "#/components/schemas/";
    private static final String REQUEST_BODY_REFERENCE_PREFIX = "#/components/requestBodies/";
    private static final String PARAMETER_REFERENCE_PREFIX = "#/components/parameters/";
    private static final String RESPONSE_REFERENCE_PREFIX = "#/components/responses/";
    private static final String HEADER_REFERENCE_PREFIX = "#/components/headers/";
    private static final String CALLBACK_REFERENCE_PREFIX = "#/components/callbacks/";

    private final List<String> publicPathPrefixes;

    public PublicApiContractFilter() {
        this(ConfigProvider.getConfig().getOptionalValues(PUBLIC_PATH_PREFIXES, String.class).orElse(List.of()));
    }

    PublicApiContractFilter(List<String> publicPathPrefixes) {
        this.publicPathPrefixes = normalizedPrefixes(publicPathPrefixes);
    }

    @Override
    public void filterOpenAPI(OpenAPI openApi) {
        if (openApi.getPaths() != null) {
            new ArrayList<>(openApi.getPaths().getPathItems().keySet()).stream()
                .filter(path -> !isPublicPath(path))
                .forEach(openApi.getPaths()::removePathItem);
        }
        retainReachableSchemas(openApi);
    }

    private boolean isPublicPath(String path) {
        return publicPathPrefixes.stream().anyMatch(prefix ->
            "/".equals(prefix) || path.equals(prefix) || path.startsWith(prefix + "/"));
    }

    private void retainReachableSchemas(OpenAPI openApi) {
        Components components = openApi.getComponents();
        if (components == null || components.getSchemas() == null) {
            return;
        }
        Map<String, Schema> schemas = components.getSchemas();
        Set<String> reachable = new HashSet<>();
        if (openApi.getPaths() != null) {
            openApi.getPaths().getPathItems().values()
                .forEach(path -> pathItem(path, components, schemas, reachable));
        }
        new ArrayList<>(schemas.keySet()).stream()
            .filter(name -> !reachable.contains(name))
            .forEach(components::removeSchema);
    }

    private void pathItem(
        PathItem path, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        parameters(path.getParameters(), components, schemas, reachable);
        Map<PathItem.HttpMethod, Operation> operations = path.getOperations();
        if (operations != null) {
            operations.values().forEach(operation -> operation(operation, components, schemas, reachable));
        }
    }

    private void operation(
        Operation operation, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        parameters(operation.getParameters(), components, schemas, reachable);
        requestBody(operation.getRequestBody(), components, schemas, reachable);
        if (operation.getResponses() != null) {
            operation.getResponses().getAPIResponses().values()
                .forEach(response -> response(response, components, schemas, reachable));
        }
        if (operation.getCallbacks() != null) {
            operation.getCallbacks().values()
                .forEach(callback -> callback(callback, components, schemas, reachable));
        }
    }

    private void callback(
        Callback callback, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        callback = resolveComponent(
            callback, CALLBACK_REFERENCE_PREFIX, components.getCallbacks());
        if (callback.getPathItems() != null) {
            callback.getPathItems().values()
                .forEach(path -> pathItem(path, components, schemas, reachable));
        }
    }

    private void response(
        APIResponse response, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        response = resolveComponent(
            response, RESPONSE_REFERENCE_PREFIX, components.getResponses());
        content(response.getContent(), schemas, reachable);
        if (response.getHeaders() != null) {
            response.getHeaders().values()
                .forEach(header -> header(header, components, schemas, reachable));
        }
    }

    private void header(
        Header header, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        header = resolveComponent(header, HEADER_REFERENCE_PREFIX, components.getHeaders());
        schema(header.getSchema(), schemas, reachable);
        content(header.getContent(), schemas, reachable);
    }

    private void requestBody(
        RequestBody requestBody, Components components, Map<String, Schema> schemas, Set<String> reachable) {
        if (requestBody != null) {
            requestBody = resolveComponent(
                requestBody, REQUEST_BODY_REFERENCE_PREFIX, components.getRequestBodies());
            content(requestBody.getContent(), schemas, reachable);
        }
    }

    private void parameters(
        List<Parameter> parameters,
        Components components,
        Map<String, Schema> schemas,
        Set<String> reachable) {
        if (parameters != null) {
            parameters.forEach(parameter -> {
                parameter = resolveComponent(
                    parameter, PARAMETER_REFERENCE_PREFIX, components.getParameters());
                schema(parameter.getSchema(), schemas, reachable);
                content(parameter.getContent(), schemas, reachable);
            });
        }
    }

    private <T extends Reference<T>> T resolveComponent(
        T value, String referencePrefix, Map<String, T> componentValues) {
        if (componentValues == null) {
            return value;
        }
        Set<String> visited = new HashSet<>();
        T resolved = value;
        while (resolved.getRef() != null && resolved.getRef().startsWith(referencePrefix)) {
            String name = resolved.getRef().substring(referencePrefix.length());
            if (!visited.add(name) || !componentValues.containsKey(name)) {
                return resolved;
            }
            resolved = componentValues.get(name);
        }
        return resolved;
    }

    private void content(Content content, Map<String, Schema> schemas, Set<String> reachable) {
        if (content != null) {
            content.getMediaTypes().values()
                .forEach(mediaType -> schema(mediaType.getSchema(), schemas, reachable));
        }
    }

    private void schema(Schema schema, Map<String, Schema> schemas, Set<String> reachable) {
        if (schema == null) {
            return;
        }
        String reference = schema.getRef();
        if (reference != null && reference.startsWith(SCHEMA_REFERENCE_PREFIX)) {
            String name = reference.substring(SCHEMA_REFERENCE_PREFIX.length());
            if (reachable.add(name)) {
                schema(schemas.get(name), schemas, reachable);
            }
        }
        schema(schema.getItems(), schemas, reachable);
        schema(schema.getNot(), schemas, reachable);
        schema(schema.getAdditionalPropertiesSchema(), schemas, reachable);
        schema(schema.getIfSchema(), schemas, reachable);
        schema(schema.getThenSchema(), schemas, reachable);
        schema(schema.getElseSchema(), schemas, reachable);
        schema(schema.getContains(), schemas, reachable);
        schema(schema.getPropertyNames(), schemas, reachable);
        schema(schema.getContentSchema(), schemas, reachable);
        schemaMap(schema.getProperties(), schemas, reachable);
        schemaMap(schema.getDependentSchemas(), schemas, reachable);
        schemaMap(schema.getPatternProperties(), schemas, reachable);
        schemaList(schema.getAllOf(), schemas, reachable);
        schemaList(schema.getAnyOf(), schemas, reachable);
        schemaList(schema.getOneOf(), schemas, reachable);
        schemaList(schema.getPrefixItems(), schemas, reachable);
        discriminator(schema.getDiscriminator(), schemas, reachable);
    }

    private void discriminator(Discriminator discriminator, Map<String, Schema> schemas, Set<String> reachable) {
        if (discriminator == null || discriminator.getMapping() == null) {
            return;
        }
        discriminator.getMapping().values().forEach(reference -> {
            String name = reference.startsWith(SCHEMA_REFERENCE_PREFIX)
                ? reference.substring(SCHEMA_REFERENCE_PREFIX.length())
                : reference;
            if (reachable.add(name)) {
                schema(schemas.get(name), schemas, reachable);
            }
        });
    }

    private void schemaMap(Map<String, Schema> values, Map<String, Schema> schemas, Set<String> reachable) {
        if (values != null) {
            values.values().forEach(value -> schema(value, schemas, reachable));
        }
    }

    private void schemaList(List<Schema> values, Map<String, Schema> schemas, Set<String> reachable) {
        if (values != null) {
            values.forEach(value -> schema(value, schemas, reachable));
        }
    }

    private static List<String> normalizedPrefixes(List<String> prefixes) {
        List<String> normalized = prefixes.stream()
            .map(String::trim)
            .filter(prefix -> !prefix.isEmpty())
            .map(prefix -> prefix.length() > 1 && prefix.endsWith("/")
                ? prefix.substring(0, prefix.length() - 1)
                : prefix)
            .toList();
        if (normalized.isEmpty()) {
            throw new IllegalStateException(PUBLIC_PATH_PREFIXES + " must declare at least one public path root");
        }
        if (normalized.stream().anyMatch(prefix -> !prefix.startsWith("/"))) {
            throw new IllegalArgumentException(PUBLIC_PATH_PREFIXES + " entries must start with '/'");
        }
        return normalized;
    }
}
