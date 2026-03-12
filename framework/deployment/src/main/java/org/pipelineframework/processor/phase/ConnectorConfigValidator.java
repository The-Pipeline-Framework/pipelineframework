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
        List<String> warnings = new ArrayList<>();
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
            validateStructure(next, knownSteps, errors, warnings);
            if (processingEnv != null) {
                validateTypes(next, processingEnv, errors);
            }
            normalized.add(next);
        }

        if (messager != null) {
            for (String warning : warnings) {
                messager.printMessage(Diagnostic.Kind.WARNING, warning);
            }
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
            connector.idempotencyMaxKeys() > 0 ? connector.idempotencyMaxKeys() : 10000,
            connector.idempotencyKeyFields(),
            broker);
    }

    private void validateStructure(
        ConnectorConfig connector,
        Set<String> knownSteps,
        List<String> errors,
        List<String> warnings
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
            warnings.add("Connector '" + connector.name() + "': source.step '" + connector.source().step()
                + "' cannot be validated because no steps are defined");
        } else if (!knownSteps.isEmpty() && !knownSteps.contains(connector.source().step())) {
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

    private String normalizeOrDefault(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim();
    }
}
