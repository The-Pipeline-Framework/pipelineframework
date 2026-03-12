package org.pipelineframework.processor.phase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import org.pipelineframework.config.connector.ConnectorBrokerConfig;
import org.pipelineframework.config.connector.ConnectorConfig;
import org.pipelineframework.config.connector.ConnectorSourceConfig;
import org.pipelineframework.config.connector.ConnectorTargetConfig;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.connector.ConnectorFailureMode;
import org.pipelineframework.connector.ConnectorSupport;
import org.pipelineframework.processor.ir.StepDefinition;

/**
 * Validates connector declarations loaded from pipeline YAML.
 */
final class ConnectorConfigValidator {
    private static final String OUTPUT_BUS = "OUTPUT_BUS";
    private static final String LIVE_INGEST = "LIVE_INGEST";
    private static final String GRPC = "GRPC";
    private static final String CONNECTOR_MAPPER = "org.pipelineframework.connector.ConnectorMapper";
    private static final String CONNECTOR_TARGET = "org.pipelineframework.connector.ConnectorTarget";

    /**
     * Validates and normalizes connector declarations from a pipeline template.
     *
     * Processes each connector in the provided template, normalizes its fields, checks structural constraints,
     * optionally performs compile-time type checks using the provided processing environment, and collects
     * diagnostics via the provided messager.
     *
     * @param templateConfig   the pipeline template containing connector declarations to validate
     * @param stepDefinitions  additional step definitions whose names are considered known for validation
     * @param processingEnv    the annotation processing environment used to resolve and validate types; may be null to skip type checks
     * @param messager         a messager for reporting warnings and errors; may be null to suppress diagnostic output
     * @return                 a list of normalized ConnectorConfig objects derived from the template's connectors
     * @throws IllegalStateException if validation produces one or more errors
     */
    List<ConnectorConfig> validate(
        PipelineTemplateConfig templateConfig,
        List<StepDefinition> stepDefinitions,
        ProcessingEnvironment processingEnv,
        Messager messager
    ) {
        if (templateConfig == null || templateConfig.connectors() == null || templateConfig.connectors().isEmpty()) {
            return List.of();
        }

        List<String> errors = new ArrayList<>();
        List<ConnectorConfig> normalized = new ArrayList<>();
        Set<String> names = new HashSet<>();
        Set<String> knownSteps = new HashSet<>();
        if (templateConfig.steps() != null) {
            templateConfig.steps().stream()
                .filter(Objects::nonNull)
                .map(step -> step.name())
                .filter(Objects::nonNull)
                .forEach(knownSteps::add);
        }
        if (stepDefinitions != null) {
            stepDefinitions.stream()
                .filter(Objects::nonNull)
                .map(StepDefinition::name)
                .filter(Objects::nonNull)
                .forEach(knownSteps::add);
        }

        for (ConnectorConfig connector : templateConfig.connectors()) {
            if (connector == null) {
                continue;
            }
            ConnectorConfig next = normalize(connector);
            if (!names.add(next.name())) {
                errors.add("Duplicate connector name '" + next.name() + "'");
            }
            validateStructure(next, knownSteps, errors);
            if (processingEnv != null && hasRequiredTypeNames(next)) {
                validateTypes(next, processingEnv, errors);
            }
            normalized.add(next);
        }
        if (!errors.isEmpty()) {
            if (messager != null) {
                for (String error : errors) {
                    messager.printMessage(Diagnostic.Kind.ERROR, error);
                }
            }
            throw new IllegalStateException(String.join("\n", errors));
        }

        return normalized;
    }

    /**
     * Normalize a connector declaration and apply canonical defaults.
     *
     * <p>The returned ConnectorConfig has canonicalized token values (transport and kind values
     * uppercased), default source/target when missing, normalized policy names, and concrete numeric
     * defaults for capacity and idempotency limits. Notable defaults: transport defaults to "GRPC",
     * source.kind defaults to "OUTPUT_BUS", target.kind defaults to "LIVE_INGEST", backpressure
     * buffer capacity defaults to 256 when non-positive, and idempotency max keys defaults to 10000
     * when non-positive.
     *
     * @param connector the original ConnectorConfig to normalize
     * @return a new ConnectorConfig with normalized transport/kind values, defaulted source/target,
     *         normalized policy names, and numeric defaults applied (buffer capacity and idempotency
     *         max keys)
     */
    private ConnectorConfig normalize(ConnectorConfig connector) {
        String transport = normalizeOrDefault(connector.transport(), GRPC).toUpperCase(Locale.ROOT);
        ConnectorSourceConfig source = connector.source() == null
            ? new ConnectorSourceConfig(OUTPUT_BUS, null, null)
            : new ConnectorSourceConfig(
                normalizeOrDefault(connector.source().kind(), OUTPUT_BUS).toUpperCase(Locale.ROOT),
                connector.source().step(),
                connector.source().type());
        ConnectorTargetConfig target = connector.target() == null
            ? new ConnectorTargetConfig(LIVE_INGEST, null, null, null)
            : new ConnectorTargetConfig(
                normalizeOrDefault(connector.target().kind(), LIVE_INGEST).toUpperCase(Locale.ROOT),
                connector.target().pipeline(),
                connector.target().type(),
                connector.target().adapter());
        ConnectorBrokerConfig broker = connector.broker() == null ? null : new ConnectorBrokerConfig(
            connector.broker().provider(),
            connector.broker().destination(),
            connector.broker().adapter());
        return new ConnectorConfig(
            connector.name(),
            connector.enabled(),
            source,
            target,
            connector.mapper(),
            transport,
            ConnectorSupport.normalizeIdempotencyPolicy(connector.idempotency()).name(),
            ConnectorSupport.normalizeBackpressurePolicy(connector.backpressure()).name(),
            ConnectorSupport.normalizeFailureMode(connector.failureMode()).name(),
            connector.backpressureBufferCapacity() > 0 ? connector.backpressureBufferCapacity() : 256,
            connector.idempotencyMaxKeys(),
            connector.idempotencyKeyFields(),
            broker);
    }

    /**
     * Validate structural requirements of a connector declaration and record any problems.
     *
     * Adds human-readable error messages to {@code errors} for violations of required fields
     * and unsupported configuration for generated v1 connectors.
     *
     * @param connector the connector configuration to validate
     * @param knownSteps set of declared pipeline step names used to verify the connector's source.step; may be empty
     * @param errors mutable list to which non-recoverable validation problems will be appended
     */
    private void validateStructure(
        ConnectorConfig connector,
        Set<String> knownSteps,
        List<String> errors
    ) {
        if (connector.name() == null || connector.name().isBlank()) {
            errors.add("Connector declarations require a non-blank name");
            return;
        }
        if (!OUTPUT_BUS.equals(connector.source().kind())) {
            errors.add("Connector '" + connector.name()
                + "': only source.kind=OUTPUT_BUS is supported for generated connectors in v1");
        }
        if (!LIVE_INGEST.equals(connector.target().kind())) {
            errors.add("Connector '" + connector.name()
                + "': only target.kind=LIVE_INGEST is supported for generated connectors in v1");
        }
        if (!GRPC.equals(connector.transport())) {
            errors.add("Connector '" + connector.name()
                + "': only transport=GRPC is supported for generated connectors in v1");
        }
        if (connector.broker() != null) {
            errors.add("Connector '" + connector.name()
                + "': broker-backed connector declarations are not generated in v1; use a manual bridge");
        }
        if (connector.source().step() == null || connector.source().step().isBlank()) {
            errors.add("Connector '" + connector.name() + "': source.step is required");
        } else if (knownSteps.isEmpty()) {
            errors.add("Connector '" + connector.name() + "': source.step '" + connector.source().step()
                + "' cannot be validated because no steps are defined");
        } else if (!knownSteps.contains(connector.source().step())) {
            errors.add("Connector '" + connector.name() + "': source.step '" + connector.source().step()
                + "' does not match any declared pipeline step");
        }
        if (connector.source().type() == null || connector.source().type().isBlank()) {
            errors.add("Connector '" + connector.name() + "': source.type is required");
        }
        if (connector.target().type() == null || connector.target().type().isBlank()) {
            errors.add("Connector '" + connector.name() + "': target.type is required");
        }
        if (connector.target().adapter() == null || connector.target().adapter().isBlank()) {
            errors.add("Connector '" + connector.name() + "': target.adapter is required");
        }
        if (!Objects.equals(connector.source().type(), connector.target().type())
            && (connector.mapper() == null || connector.mapper().isBlank())) {
            errors.add("Connector '" + connector.name()
                + "': mapper is required when source.type and target.type differ");
        }
    }

    private boolean hasRequiredTypeNames(ConnectorConfig connector) {
        return hasText(connector.source().type())
            && hasText(connector.target().type())
            && hasText(connector.target().adapter());
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Validate that the connector's referenced types resolve and that its adapter and optional mapper
     * implement the expected parameterized interfaces.
     *
     * Validations performed:
     * - source.type, target.type, and target.adapter must be resolvable; unresolved names append errors.
     * - target.adapter must implement ConnectorTarget<target.type>; mismatch appends an error.
     * - if a mapper is specified, it must be resolvable and implement ConnectorMapper<source.type, target.type>;
     *   unresolved or non-conforming mappers append errors.
     *
     * @param connector the connector configuration to validate
     * @param processingEnv the annotation-processing environment used to resolve types
     * @param errors a mutable list to which human-readable validation error messages will be appended
     */
    private void validateTypes(ConnectorConfig connector, ProcessingEnvironment processingEnv, List<String> errors) {
        Elements elements = processingEnv.getElementUtils();
        Types types = processingEnv.getTypeUtils();
        TypeElement sourceType = elements.getTypeElement(connector.source().type());
        TypeElement targetType = elements.getTypeElement(connector.target().type());
        TypeElement adapterType = elements.getTypeElement(connector.target().adapter());
        if (sourceType == null) {
            errors.add("Connector '" + connector.name() + "': source.type '" + connector.source().type()
                + "' could not be resolved");
        }
        if (targetType == null) {
            errors.add("Connector '" + connector.name() + "': target.type '" + connector.target().type()
                + "' could not be resolved");
        }
        if (adapterType == null) {
            errors.add("Connector '" + connector.name() + "': target.adapter '" + connector.target().adapter()
                + "' could not be resolved");
        }
        if (sourceType == null || targetType == null || adapterType == null) {
            return;
        }
        if (!implementsParameterizedInterface(adapterType.asType(), CONNECTOR_TARGET, List.of(targetType.asType()), elements, types)) {
            errors.add("Connector '" + connector.name() + "': target.adapter '" + connector.target().adapter()
                + "' must implement ConnectorTarget<" + connector.target().type() + ">");
        }
        if (connector.mapper() == null || connector.mapper().isBlank()) {
            return;
        }
        TypeElement mapperType = elements.getTypeElement(connector.mapper());
        if (mapperType == null) {
            errors.add("Connector '" + connector.name() + "': mapper '" + connector.mapper()
                + "' could not be resolved");
            return;
        }
        if (!implementsParameterizedInterface(
            mapperType.asType(),
            CONNECTOR_MAPPER,
            List.of(sourceType.asType(), targetType.asType()),
            elements,
            types)) {
            errors.add("Connector '" + connector.name() + "': mapper '" + connector.mapper()
                + "' must implement ConnectorMapper<" + connector.source().type() + ", " + connector.target().type() + ">");
        }
    }

    /**
     * Checks whether a given type implements a parameterized interface with the specified type arguments.
     *
     * @param candidate the type to inspect
     * @param interfaceName the fully-qualified name of the interface to check for
     * @param expectedArguments the expected type argument mirrors for the interface (order-sensitive)
     * @param elements utility for resolving type elements
     * @param types utility for type comparisons and erasure
     * @return `true` if `candidate` implements the named interface whose type arguments match `expectedArguments`, `false` otherwise
     */
    private boolean implementsParameterizedInterface(
        TypeMirror candidate,
        String interfaceName,
        List<TypeMirror> expectedArguments,
        Elements elements,
        Types types
    ) {
        TypeElement targetInterface = elements.getTypeElement(interfaceName);
        if (candidate == null || targetInterface == null) {
            return false;
        }
        return implementsParameterizedInterface(
            candidate,
            types.erasure(targetInterface.asType()),
            expectedArguments,
            types,
            new HashSet<>());
    }

    /**
     * Determines whether the given candidate type or any of its supertypes implements the target
     * interface with the specified type arguments (comparison performed on erasures).
     *
     * @param candidate the type to inspect
     * @param targetErasure the erasure of the target interface type to match against
     * @param expectedArguments the expected type arguments for the target interface
     * @param types utility for type operations
     * @param visited a set of type identity keys used to avoid infinite recursion; the method adds the candidate key to this set
     * @return true if the candidate or any direct supertype implements the target interface with matching type arguments, false otherwise
     */
    private boolean implementsParameterizedInterface(
        TypeMirror candidate,
        TypeMirror targetErasure,
        List<TypeMirror> expectedArguments,
        Types types,
        Set<String> visited
    ) {
        String candidateKey = candidate.toString();
        if (!visited.add(candidateKey)) {
            return false;
        }
        if (types.isSameType(types.erasure(candidate), targetErasure)) {
            if (!(candidate instanceof DeclaredType declaredType)) {
                return false;
            }
            List<? extends TypeMirror> typeArguments = declaredType.getTypeArguments();
            if (typeArguments.size() != expectedArguments.size()) {
                return false;
            }
            for (int i = 0; i < typeArguments.size(); i++) {
                if (!types.isSameType(types.erasure(typeArguments.get(i)), types.erasure(expectedArguments.get(i)))) {
                    return false;
                }
            }
            return true;
        }
        for (TypeMirror superType : types.directSupertypes(candidate)) {
            if (implementsParameterizedInterface(superType, targetErasure, expectedArguments, types, visited)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return the trimmed input string or the provided fallback when the input is null or blank.
     *
     * @param value the input string to normalize
     * @param fallback the value to return when {@code value} is null or blank
     * @return the trimmed input string, or {@code fallback} if {@code value} is null or blank
     */
    private String normalizeOrDefault(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim();
    }
}
