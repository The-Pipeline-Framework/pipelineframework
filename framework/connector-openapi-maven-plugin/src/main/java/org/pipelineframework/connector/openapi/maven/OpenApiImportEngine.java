package org.pipelineframework.connector.openapi.maven;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorConfigFieldDescriptor;
import org.pipelineframework.connector.ConnectorConfigSchemaDescriptor;
import org.pipelineframework.connector.ConnectorConfigValueType;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorOperationTypeContract;
import org.pipelineframework.connector.ConnectorProviderArtifactDescriptor;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.connector.http.HttpAuthorizationTarget;
import org.pipelineframework.connector.http.HttpOperationCatalog;
import org.pipelineframework.connector.http.HttpOperationPin;
import org.pipelineframework.connector.http.HttpParameterLocation;
import org.pipelineframework.connector.http.HttpParameterPin;
import org.pipelineframework.connector.http.HttpParameterStyle;
import org.pipelineframework.connector.http.HttpPinnedJson;
import org.pipelineframework.connector.http.HttpProviderIdempotencyKeyTarget;
import org.pipelineframework.connector.http.HttpRequestBodyPin;
import org.pipelineframework.connector.http.HttpResponseOutcome;
import org.pipelineframework.connector.http.HttpResponsePin;
import org.pipelineframework.connector.http.HttpSecurityConstraint;
import org.pipelineframework.connector.http.HttpSecurityRequirement;
import org.pipelineframework.connector.http.HttpWireSchema;
import org.pipelineframework.connector.importer.ConnectorImportResource;

/** Converts explicit OpenAPI import choices into ordinary pinned HTTP capabilities. */
final class OpenApiImportEngine {
    static final String PIN_PATH = "META-INF/pipeline/http-operations.json";
    static final String PROVENANCE_PATH = "META-INF/pipeline/connector-operation-provenance.json";
    private static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("http.client");
    private static final ObjectMapper JSON = Json.mapper();

    ImportedArtifacts importContract(AbstractOpenApiImportMojo.LoadedImport loaded) {
        OpenApiImportConfiguration configuration = loaded.configuration();
        require(configuration.schemaVersion == 1, "OpenAPI import schemaVersion must be 1");
        String importId = text(configuration.importId, "OpenAPI import ID");
        require(configuration.operations != null && !configuration.operations.isEmpty(),
            "OpenAPI import must select at least one operation");
        if (configuration.source.closureSha256 != null && !configuration.source.closureSha256.isBlank()) {
            require(loaded.closure().digest().equalsIgnoreCase(configuration.source.closureSha256),
                "OpenAPI closure digest does not match source.closureSha256");
        }
        OpenAPI contract = parse(loaded.closure());
        List<HttpOperationPin> pins = configuration.operations.stream()
            .map(selection -> operation(importId, contract, loaded.closure(), selection))
            .sorted(Comparator.comparing(HttpOperationPin::identity)).toList();
        List<ConnectorOperationDescriptor> operations = pins.stream().map(OpenApiImportEngine::descriptor).toList();
        var providerSchema = new ConnectorConfigSchemaDescriptor("http.client.provider", 1,
            List.of(
                new ConnectorConfigFieldDescriptor("connection", ConnectorConfigValueType.CONNECTION_REF, true),
                new ConnectorConfigFieldDescriptor("maxRequestBytes", ConnectorConfigValueType.INTEGER, false),
                new ConnectorConfigFieldDescriptor("maxResponseBytes", ConnectorConfigValueType.INTEGER, false),
                new ConnectorConfigFieldDescriptor("requestTimeout", ConnectorConfigValueType.DURATION, false)));
        var provider = new ConnectorProviderArtifactDescriptor(new ConnectorProviderDescriptor(
            PROVIDER_ID, new ConnectorProviderVersion(1, 0), Optional.of(providerSchema)), operations, List.of());
        String provenance = provenance(importId, loaded.closure(), contract, pins, configuration.operations);
        return new ImportedArtifacts(provider, List.of(
            new ConnectorImportResource(PIN_PATH, new HttpOperationCatalog(pins).json()),
            new ConnectorImportResource(PROVENANCE_PATH, provenance)), discovery(contract, loaded.closure()));
    }

    String discover(OpenApiContractClosure.Resolved closure) {
        return discovery(parse(closure), closure);
    }

    private static OpenAPI parse(OpenApiContractClosure.Resolved closure) {
        ParseOptions options = new ParseOptions();
        options.setResolve(true);
        options.setResolveFully(true);
        options.setResolveCombinators(false);
        var result = new OpenAPIV3Parser().readLocation(closure.root().toUri().toString(), List.of(), options);
        if (result.getOpenAPI() == null || result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Unable to parse OpenAPI contract: "
                + String.join("; ", Optional.ofNullable(result.getMessages()).orElse(List.of())));
        }
        return result.getOpenAPI();
    }

    private static HttpOperationPin operation(
        String importId,
        OpenAPI contract,
        OpenApiContractClosure.Resolved closure,
        OpenApiImportConfiguration.OperationSelection selection
    ) {
        require(selection != null && selection.source != null, "OpenAPI operation selection requires source");
        String path = text(selection.source.path, "OpenAPI source path");
        String method = text(selection.source.method, "OpenAPI source method").toUpperCase(Locale.ROOT);
        PathItem item = Optional.ofNullable(contract.getPaths()).map(paths -> paths.get(path)).orElseThrow(() ->
            new IllegalArgumentException("selected OpenAPI path was not discovered: " + path));
        PathItem.HttpMethod httpMethod;
        try {
            httpMethod = PathItem.HttpMethod.valueOf(method);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("unsupported selected OpenAPI method " + method, failure);
        }
        Operation source = Optional.ofNullable(item.readOperationsMap().get(httpMethod)).orElseThrow(() ->
            new IllegalArgumentException("selected OpenAPI operation was not discovered: " + method + " " + path));
        require(text(selection.source.operationId, "source operationId").equals(source.getOperationId()),
            "selected OpenAPI operationId does not match method/path guard");
        require("APPLICATION_BOUND".equals(selection.server),
            "OpenAPI operations must select server: APPLICATION_BOUND");
        ConnectorOperationKind kind = kind(selection.kind);
        String operation = text(selection.operation, "TPF operation identity");
        require(selection.version > 0, "TPF operation version must be positive");
        String input = text(selection.input, "TPF operation input type");
        String output = text(selection.output, "TPF operation output type");
        Request request = request(item, source, selection.request, operation);
        List<HttpResponsePin> responses = responses(source, selection.responses, kind, operation);
        HttpSecurityConstraint security = security(contract, source, selection.security);
        Optional<HttpProviderIdempotencyKeyTarget> idempotency = idempotency(selection.providerIdempotencyKey, kind);
        return new HttpOperationPin(operation, kind, selection.version, input, output, method, path,
            request.parameters(), request.body(), responses, security, request.schema(), request.mappingKey(),
            idempotency, closure.digest());
    }

    private static Request request(
        PathItem item,
        Operation operation,
        OpenApiImportConfiguration.RequestSelection selected,
        String operationId
    ) {
        require(selected != null, "OpenAPI operation requires an explicit request selection");
        Map<String, String> sources = Optional.ofNullable(selected.parameterSources).orElse(Map.of());
        List<Parameter> parameters = new ArrayList<>();
        if (item.getParameters() != null) parameters.addAll(item.getParameters());
        if (operation.getParameters() != null) parameters.addAll(operation.getParameters());
        List<HttpParameterPin> pins = new ArrayList<>();
        ObjectNode combined = JSON.createObjectNode();
        combined.put("type", "object");
        combined.put("additionalProperties", false);
        ObjectNode properties = combined.putObject("properties");
        ArrayNode required = combined.putArray("required");
        for (Parameter parameter : parameters) {
            String sourcePath = sources.getOrDefault(parameter.getName(), parameter.getName());
            require(!sourcePath.contains("."), "initial OpenAPI parameter source paths must be top-level fields");
            JsonNode schema = schema(parameter.getSchema(), "parameter " + parameter.getName());
            properties.set(sourcePath, schema);
            boolean isRequired = Boolean.TRUE.equals(parameter.getRequired()) || "path".equals(parameter.getIn());
            if (isRequired) required.add(sourcePath);
            HttpParameterLocation location = HttpParameterLocation.valueOf(parameter.getIn().toUpperCase(Locale.ROOT));
            HttpParameterStyle style = style(parameter.getStyle(), location);
            boolean explode = parameter.getExplode() != null ? parameter.getExplode() : style == HttpParameterStyle.FORM;
            pins.add(new HttpParameterPin(parameter.getName(), location, sourcePath, style, explode, isRequired,
                Boolean.TRUE.equals(parameter.getAllowReserved()), new HttpWireSchema(HttpPinnedJson.canonicalize(schema))));
        }
        Optional<HttpRequestBodyPin> body = Optional.empty();
        if (operation.getRequestBody() != null) {
            String media = text(selected.mediaType, "OpenAPI request media type").toLowerCase(Locale.ROOT);
            Schema<?> bodySchema = media(operation.getRequestBody().getContent(), media, "request").getSchema();
            JsonNode schema = schema(bodySchema, "request body");
            Optional<String> bodyPath;
            if (pins.isEmpty() && (selected.bodyPath == null || selected.bodyPath.isBlank())) {
                combined = (ObjectNode) schema.deepCopy();
                bodyPath = Optional.empty();
            } else {
                String path = selected.bodyPath == null || selected.bodyPath.isBlank() ? "body" : selected.bodyPath;
                require(!path.contains("."), "initial OpenAPI request body paths must be top-level fields");
                properties.set(path, schema);
                if (Boolean.TRUE.equals(operation.getRequestBody().getRequired())) required.add(path);
                bodyPath = Optional.of(path);
            }
            body = Optional.of(new HttpRequestBodyPin(media, bodyPath,
                Boolean.TRUE.equals(operation.getRequestBody().getRequired()),
                new HttpWireSchema(HttpPinnedJson.canonicalize(schema))));
        } else {
            require(selected.mediaType == null || selected.mediaType.isBlank(),
                "request mediaType was selected for an operation without a request body");
        }
        String mapping = selected.representation == null || selected.representation.isBlank()
            ? "http." + operationId + ".request" : selected.representation;
        return new Request(List.copyOf(pins), body,
            new HttpWireSchema(HttpPinnedJson.canonicalize(combined)), mapping);
    }

    private static List<HttpResponsePin> responses(
        Operation operation,
        List<OpenApiImportConfiguration.ResponseSelection> selections,
        ConnectorOperationKind kind,
        String operationId
    ) {
        require(selections != null && !selections.isEmpty(), "OpenAPI operation requires response selections");
        List<HttpResponsePin> result = new ArrayList<>();
        for (OpenApiImportConfiguration.ResponseSelection selected : selections) {
            String status = text(selected.status, "OpenAPI response status");
            var response = Optional.ofNullable(operation.getResponses()).map(values -> values.get(status)).orElseThrow(() ->
                new IllegalArgumentException("selected OpenAPI response was not discovered: " + status));
            HttpResponseOutcome outcome = HttpResponseOutcome.valueOf(text(selected.outcome,
                "HTTP response outcome").toUpperCase(Locale.ROOT));
            Optional<String> mediaType = Optional.ofNullable(selected.mediaType).filter(value -> !value.isBlank())
                .map(value -> value.toLowerCase(Locale.ROOT));
            boolean success = outcome == HttpResponseOutcome.RESULT || outcome == HttpResponseOutcome.SUCCEEDED;
            Optional<HttpWireSchema> wireSchema = Optional.empty();
            if (success) {
                var media = media(response.getContent(), mediaType.orElseThrow(() ->
                    new IllegalArgumentException("successful OpenAPI response requires mediaType")), "response " + status);
                wireSchema = Optional.of(new HttpWireSchema(HttpPinnedJson.canonicalize(
                    schema(media.getSchema(), "response " + status))));
            }
            Optional<String> mapping = success ? Optional.of(Optional.ofNullable(selected.representation)
                .filter(value -> !value.isBlank()).orElse("http." + operationId + ".response." + status)) : Optional.empty();
            Optional<String> code = success ? Optional.empty() : Optional.of(text(selected.code, "failure response code"));
            Optional<CommandMachineConfirmation> confirmation = outcome == HttpResponseOutcome.SUCCEEDED
                ? Optional.of(CommandMachineConfirmation.valueOf(text(selected.confirmation,
                    "Command response confirmation").toUpperCase(Locale.ROOT))) : Optional.empty();
            result.add(new HttpResponsePin(status, mediaType, outcome, mapping, code, confirmation, wireSchema));
        }
        return List.copyOf(result);
    }

    private static HttpSecurityConstraint security(
        OpenAPI contract,
        Operation operation,
        OpenApiImportConfiguration.SecuritySelection selected
    ) {
        require(selected != null, "OpenAPI operation requires an explicit security selection");
        List<SecurityRequirement> advertised = operation.getSecurity() != null
            ? operation.getSecurity() : Optional.ofNullable(contract.getSecurity()).orElse(List.of());
        Map<String, List<String>> required = Optional.ofNullable(selected.require).orElse(Map.of());
        if (selected.none) {
            require(required.isEmpty(), "security NONE cannot also require schemes");
            require(advertised.stream().anyMatch(Map::isEmpty) || advertised.isEmpty(),
                "OpenAPI operation does not advertise an unauthenticated security alternative");
            return HttpSecurityConstraint.none();
        }
        require(!required.isEmpty(), "security selection must choose NONE or one advertised alternative");
        require(advertised.stream().anyMatch(value -> value.keySet().equals(required.keySet())
            && value.entrySet().stream().allMatch(entry -> List.copyOf(entry.getValue())
                .equals(Optional.ofNullable(required.get(entry.getKey())).orElse(List.of())))),
            "selected security requirement is not an advertised OpenAPI alternative");
        List<HttpSecurityRequirement> requirements = required.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(entry -> securityRequirement(contract, entry.getKey(), entry.getValue())).toList();
        return new HttpSecurityConstraint(requirements);
    }

    private static HttpSecurityRequirement securityRequirement(OpenAPI contract, String name, List<String> scopes) {
        SecurityScheme scheme = Optional.ofNullable(contract.getComponents()).map(value -> value.getSecuritySchemes())
            .map(values -> values.get(name)).orElseThrow(() ->
                new IllegalArgumentException("selected OpenAPI security scheme was not discovered: " + name));
        HttpAuthorizationTarget target = switch (scheme.getType()) {
            case APIKEY -> new HttpAuthorizationTarget(
                HttpParameterLocation.valueOf(scheme.getIn().name()), text(scheme.getName(), "API-key wire name"));
            case HTTP, OAUTH2, OPENIDCONNECT -> new HttpAuthorizationTarget(HttpParameterLocation.HEADER, "Authorization");
            default -> throw new IllegalArgumentException("unsupported OpenAPI security scheme type " + scheme.getType());
        };
        return new HttpSecurityRequirement(name, scopes, List.of(target));
    }

    private static Optional<HttpProviderIdempotencyKeyTarget> idempotency(
        OpenApiImportConfiguration.ProviderIdempotencyKey selected,
        ConnectorOperationKind kind
    ) {
        if (selected == null) return Optional.empty();
        require(kind == ConnectorOperationKind.COMMAND,
            "provider idempotency-key projection requires explicit COMMAND classification");
        return Optional.of(new HttpProviderIdempotencyKeyTarget(
            HttpParameterLocation.valueOf(text(selected.location, "idempotency-key location").toUpperCase(Locale.ROOT)),
            text(selected.name, "idempotency-key name")));
    }

    private static ConnectorOperationDescriptor descriptor(HttpOperationPin pin) {
        CommandMachineConfirmation maximumConfirmation = pin.responses().stream()
            .map(HttpResponsePin::confirmation)
            .flatMap(Optional::stream)
            .max(Comparator.comparingInt(Enum::ordinal))
            .orElse(CommandMachineConfirmation.NONE);
        Optional<CommandCapabilities> command = pin.kind() == ConnectorOperationKind.COMMAND
            ? Optional.of(new CommandCapabilities(true, pin.providerIdempotencyKey().isPresent(), false,
                CommandExecutionPosture.UNSPECIFIED, maximumConfirmation, false, Set.of()))
            : Optional.empty();
        Optional<QueryCapabilities> query = pin.kind() == ConnectorOperationKind.QUERY
            ? Optional.of(QueryCapabilities.conservative()) : Optional.empty();
        return new ConnectorOperationDescriptor(pin.operation(), pin.kind(), pin.majorVersion(), Optional.empty(),
            command, query, pin.kind() == ConnectorOperationKind.QUERY
                ? Optional.of(QueryOperationCardinality.ONE_TO_ONE) : Optional.empty(),
            Optional.of(new ConnectorOperationTypeContract(pin.inputType(), Optional.of(pin.outputType()))));
    }

    private static String discovery(OpenAPI contract, OpenApiContractClosure.Resolved closure) {
        ObjectNode root = JSON.createObjectNode();
        root.put("schemaVersion", 1);
        root.put("openapiVersion", closure.version());
        root.put("closureSha256", closure.digest());
        ArrayNode operations = root.putArray("operations");
        Optional.ofNullable(contract.getPaths()).orElse(new io.swagger.v3.oas.models.Paths()).entrySet().stream()
            .sorted(Map.Entry.comparingByKey()).forEach(path -> path.getValue().readOperationsMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                    ObjectNode operation = operations.addObject();
                    operation.put("method", entry.getKey().name());
                    operation.put("path", path.getKey());
                    if (entry.getValue().getOperationId() != null) {
                        operation.put("operationId", entry.getValue().getOperationId());
                    }
                }));
        return HttpPinnedJson.canonicalize(root) + "\n";
    }

    private static String provenance(
        String importId,
        OpenApiContractClosure.Resolved closure,
        OpenAPI contract,
        List<HttpOperationPin> pins,
        List<OpenApiImportConfiguration.OperationSelection> selections
    ) {
        ObjectNode root = JSON.createObjectNode();
        root.put("schemaVersion", 1);
        ArrayNode imports = root.putArray("imports");
        ObjectNode imported = imports.addObject();
        imported.put("sourceKind", "OPENAPI");
        imported.put("provider", PROVIDER_ID.value());
        imported.put("importId", importId);
        imported.put("openapiVersion", closure.version());
        imported.put("rootSha256", closure.rootDigest());
        imported.put("closureSha256", closure.digest());
        if (!closure.acquiredRootDigest().isBlank()) {
            imported.put("acquiredRootSha256", closure.acquiredRootDigest());
            imported.put("acquiredClosureSha256", closure.acquiredClosureDigest());
        }
        imported.put("serverHintFingerprint", HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(
            JSON.valueToTree(Optional.ofNullable(contract.getServers()).orElse(List.of())))));
        ArrayNode operations = imported.putArray("operations");
        Map<String, OpenApiImportConfiguration.OperationSelection> byIdentity = new LinkedHashMap<>();
        selections.forEach(value -> byIdentity.put(kind(value.kind).value() + ":" + value.operation
            + ":" + value.version, value));
        pins.forEach(pin -> {
            var selection = Optional.ofNullable(byIdentity.get(pin.identity())).orElseThrow(() ->
                new IllegalStateException("missing import selection for normalized HTTP operation " + pin.identity()));
            ObjectNode operation = operations.addObject();
            operation.put("operation", pin.operation());
            operation.put("kind", pin.kind().value());
            operation.put("majorVersion", pin.majorVersion());
            operation.put("sourceOperationId", selection.source.operationId);
            operation.put("method", pin.method());
            operation.put("path", pin.relativePathTemplate());
            operation.put("canonicalInput", pin.inputType());
            operation.put("canonicalOutput", pin.outputType());
            operation.put("wireInputFingerprint", pin.requestSchema().sha256());
            operation.put("requestMappingKey", pin.requestMappingKey());
            operation.put("securityConstraintFingerprint", HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(
                JSON.valueToTree(pin.security()))));
            operation.put("providerIdempotencyKey", pin.providerIdempotencyKey()
                .map(value -> value.location().name() + ":" + value.name()).orElse(""));
            ArrayNode responses = operation.putArray("responses");
            pin.responses().forEach(response -> {
                ObjectNode value = responses.addObject();
                value.put("status", response.status());
                value.put("outcome", response.outcome().name());
                response.mediaType().ifPresent(media -> value.put("mediaType", media));
                response.mappingKey().ifPresent(key -> value.put("mappingKey", key));
                response.schema().ifPresent(schema -> value.put("wireFingerprint", schema.sha256()));
            });
            var sourceOperation = Optional.ofNullable(contract.getPaths()).map(paths -> paths.get(pin.relativePathTemplate()))
                .map(PathItem::readOperationsMap)
                .map(values -> values.get(PathItem.HttpMethod.valueOf(pin.method())))
                .orElseThrow();
            operation.put("vendorExtensionsFingerprint", HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(
                JSON.valueToTree(Optional.ofNullable(sourceOperation.getExtensions()).orElse(Map.of())))));
            operation.put("httpOperationPinFingerprint", pin.operationFingerprint());
        });
        return HttpPinnedJson.canonicalize(root) + "\n";
    }

    private static io.swagger.v3.oas.models.media.MediaType media(Content content, String selected, String subject) {
        return Optional.ofNullable(content).map(value -> value.get(selected)).orElseThrow(() ->
            new IllegalArgumentException("selected " + subject + " media type was not discovered: " + selected));
    }

    private static JsonNode schema(Schema<?> schema, String subject) {
        if (schema == null) throw new IllegalArgumentException("OpenAPI " + subject + " requires a schema");
        JsonNode result = JSON.valueToTree(schema);
        if (result == null || !result.isObject()) throw new IllegalArgumentException("OpenAPI " + subject + " schema is invalid");
        ObjectNode normalized = (ObjectNode) normalizeSchema(result);
        restoreSchemaTypes(normalized, schema);
        return normalized;
    }

    private static void restoreSchemaTypes(JsonNode target, Schema<?> source) {
        if (!(target instanceof ObjectNode object) || source == null) return;
        if (!object.has("type") && source.getType() != null) object.put("type", source.getType());
        if (!object.has("type") && source.getTypes() != null && !source.getTypes().isEmpty()) {
            if (source.getTypes().size() == 1) object.put("type", source.getTypes().iterator().next());
            else {
                ArrayNode types = object.putArray("type");
                source.getTypes().stream().sorted().forEach(types::add);
            }
        }
        if (source.getProperties() != null && object.path("properties").isObject()) {
            source.getProperties().forEach((name, property) ->
                restoreSchemaTypes(object.path("properties").path(name), (Schema<?>) property));
        }
        restoreSchemaTypes(object.path("items"), source.getItems());
        restoreSchemaList(object.path("allOf"), source.getAllOf());
        restoreSchemaList(object.path("anyOf"), source.getAnyOf());
        restoreSchemaList(object.path("oneOf"), source.getOneOf());
        if (source.getAdditionalProperties() instanceof Schema<?> additional) {
            restoreSchemaTypes(object.path("additionalProperties"), additional);
        }
    }

    private static void restoreSchemaList(JsonNode target, List<Schema> source) {
        if (!target.isArray() || source == null) return;
        for (int index = 0; index < Math.min(target.size(), source.size()); index++) {
            restoreSchemaTypes(target.get(index), source.get(index));
        }
    }

    private static JsonNode normalizeSchema(JsonNode value) {
        if (value.isArray()) {
            ArrayNode result = JSON.createArrayNode();
            value.forEach(child -> result.add(normalizeSchema(child)));
            return result;
        }
        if (!value.isObject()) return value.deepCopy();
        ObjectNode result = JSON.createObjectNode();
        value.fields().forEachRemaining(entry -> {
            if (!"types".equals(entry.getKey()) && !entry.getValue().isNull()) {
                result.set(entry.getKey(), normalizeSchema(entry.getValue()));
            }
        });
        if (!result.has("type") && value.path("types").isArray()) {
            if (value.path("types").size() == 1) {
                result.put("type", value.path("types").get(0).asText());
            } else {
                result.set("type", value.path("types").deepCopy());
            }
        }
        if (result.path("nullable").asBoolean(false) && result.path("type").isTextual()) {
            ArrayNode types = JSON.createArrayNode();
            types.add(result.path("type").textValue());
            types.add("null");
            result.set("type", types);
        }
        result.remove("nullable");
        return result;
    }

    private static HttpParameterStyle style(Parameter.StyleEnum style, HttpParameterLocation location) {
        String value = style == null ? switch (location) {
            case PATH, HEADER -> "simple";
            case QUERY, COOKIE -> "form";
        } : style.toString();
        return HttpParameterStyle.valueOf(value.replace('-', '_').toUpperCase(Locale.ROOT));
    }

    private static ConnectorOperationKind kind(String value) {
        String checked = text(value, "TPF operation kind").toUpperCase(Locale.ROOT);
        require("QUERY".equals(checked) || "COMMAND".equals(checked),
            "OpenAPI import kind must be explicitly QUERY or COMMAND");
        return "QUERY".equals(checked) ? ConnectorOperationKind.QUERY : ConnectorOperationKind.COMMAND;
    }

    private static String text(String value, String subject) {
        String checked = value == null ? "" : value.trim();
        if (checked.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return checked;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    record Request(
        List<HttpParameterPin> parameters,
        Optional<HttpRequestBodyPin> body,
        HttpWireSchema schema,
        String mappingKey
    ) {
    }

    record ImportedArtifacts(
        ConnectorProviderArtifactDescriptor provider,
        List<ConnectorImportResource> resources,
        String discoveryReport
    ) {
    }
}
