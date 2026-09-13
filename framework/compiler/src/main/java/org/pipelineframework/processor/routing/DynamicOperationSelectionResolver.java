package org.pipelineframework.processor.routing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.config.pipeline.PipelineYamlCallable;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorConfigSchemaDescriptor;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderManifestCatalog;
import org.pipelineframework.connector.ConnectorProviderManifestLoader;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryOperationCardinality;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.DynamicOperationSelection;

/** Resolves a dynamic operation catalogue entirely during compilation. */
public final class DynamicOperationSelectionResolver {
    private final ConnectorProviderManifestCatalog providerCatalog;

    public DynamicOperationSelectionResolver() {
        this(ConnectorProviderManifestLoader.load(
            ConnectorProviderManifestLoader.metadataClassLoader(DynamicOperationSelectionResolver.class)));
    }

    DynamicOperationSelectionResolver(ConnectorProviderManifestCatalog providerCatalog) {
        this.providerCatalog = providerCatalog;
    }

    public DynamicOperationSelection resolve(
        PipelineYamlConfig config,
        PipelineTemplateConfig template,
        PipelineReference definition,
        String dispatchStepName,
        String sourceStepName,
        String runtimeStepId
    ) {
        List<PipelineYamlStep> owner = definition.logicalId().equals("$root")
            ? config.steps()
            : Optional.ofNullable(config.localPipelines().get(definition.logicalId()))
                .orElseThrow(() -> new IllegalArgumentException(
                    "dynamic operation definition '" + definition.logicalId() + "' is not linked"));
        PipelineYamlStep dispatch = requireStep(owner, dispatchStepName, "dynamic operation");
        String declaredSource = dispatch.dynamicOperation().orElseThrow(() -> new IllegalArgumentException(
            "step '" + dispatchStepName + "' has no dynamic operation selection")).from();
        if (!sourceStepName.equals(declaredSource)) {
            throw new IllegalArgumentException("dynamic operation source changed after parsing: expected '"
                + sourceStepName + "' but linked definition declares '" + declaredSource + "'");
        }
        PipelineYamlStep source = requireStep(owner, sourceStepName, "callable source");
        if (source.callables().isEmpty()) {
            throw new IllegalArgumentException("dynamic operation callable source '" + sourceStepName
                + "' exposes no capabilities");
        }
        V3JavaTypeResolver typeResolver = new V3JavaTypeResolver(template);
        List<DynamicOperationSelection.CallableSelection> callables = new ArrayList<>();
        source.callables().values().stream().sorted(Comparator.comparing(PipelineYamlCallable::alias))
            .forEach(callable -> callables.add(resolveCallable(
                config, typeResolver, definition, sourceStepName, runtimeStepId, callable)));
        return new DynamicOperationSelection(definition, dispatchStepName, sourceStepName, runtimeStepId, callables);
    }

    private DynamicOperationSelection.CallableSelection resolveCallable(
        PipelineYamlConfig config,
        V3JavaTypeResolver typeResolver,
        PipelineReference definition,
        String sourceStepName,
        String runtimeStepId,
        PipelineYamlCallable callable
    ) {
        PipelineYamlConnectorBinding binding = Optional.ofNullable(config.connectors().get(callable.using()))
            .orElseThrow(() -> new IllegalArgumentException(
                "unknown callable connector binding '" + callable.using() + "'"));
        ConnectorProviderId providerId = ConnectorProviderId.of(binding.provider());
        ConnectorOperationDescriptor descriptor = providerCatalog.requireOperation(
            providerId, binding.version(), callable.operation(), callable.kind(), callable.operationVersion());
        if (ConnectorOperationKind.QUERY.equals(descriptor.kind())
            && descriptor.queryCardinality().orElseThrow() != QueryOperationCardinality.ONE_TO_ONE) {
            throw new IllegalArgumentException("dynamic callable Query must be ONE_TO_ONE: "
                + callable.using() + "/" + callable.operation());
        }
        var contract = descriptor.typeContract().orElseThrow(() -> new IllegalArgumentException(
            "callable operation has no canonical type contract: "
                + callable.using() + "/" + callable.operation()));
        String publishedOutputType = contract.outputType().orElseThrow(() -> new IllegalArgumentException(
            "callable operation has no output contract: " + callable.using() + "/" + callable.operation()));
        ClassName inputClass = resolveJavaType(typeResolver, config.basePackage(), callable.input());
        ClassName outputClass = resolveJavaType(typeResolver, config.basePackage(), publishedOutputType);
        String outputType = typeResolver.semanticType(outputClass).orElse(publishedOutputType);
        requireCanonicalMatch(
            typeResolver, config.basePackage(), callable.input(), contract.inputType(), "input", callable);
        providerCatalog.validateOperationConfiguration(
            providerId, binding.version(), callable.operation(), callable.kind(), callable.operationVersion(),
            new ConnectorConfigurationDocument(callable.config()), "callable operation '" + callable.alias() + "'");

        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            providerId, descriptor.id(), descriptor.kind(), descriptor.majorVersion());
        ConnectorOperationSelection operation;
        if (ConnectorOperationKind.QUERY.equals(identity.kind())) {
            operation = ConnectorOperationSelection.query(
                sourceStepName + ":" + callable.alias(), ConnectorBindingName.of(binding.name()), identity,
                binding.version(), callable.config(), new ConnectorOperationSelection.QuerySelection(
                    QueryOperationCardinality.ONE_TO_ONE,
                    descriptor.queryCapabilities().orElse(QueryCapabilities.conservative()),
                    Optional.<Duration>empty(), Map.of(), List.of()));
        } else {
            CommandPolicy policy = commandPolicy(callable.policy());
            providerCatalog.validateCommandPolicy(identity, binding.version(), policy);
            operation = ConnectorOperationSelection.command(
                sourceStepName + ":" + callable.alias(), ConnectorBindingName.of(binding.name()), identity,
                binding.version(), callable.config(), new ConnectorOperationSelection.CommandSelection(
                    ClassName.bestGuess(callable.commandIdGenerator().orElseThrow(() ->
                        new IllegalArgumentException("Command callable '" + callable.alias()
                            + "' requires application-selected commandIdGenerator"))),
                    CommandDuplicatePolicy.fromString(callable.duplicatePolicy()), policy));
        }
        operation = operation.withLinkedIdentity(definition, runtimeStepId + ":" + callable.alias());
        return new DynamicOperationSelection.CallableSelection(
            callable.alias(), operation, callable.input(), inputClass,
            outputType, outputClass, callable.trustedArguments(),
            connectorConfigurationDigest(binding));
    }

    private String connectorConfigurationDigest(PipelineYamlConnectorBinding binding) {
        ConnectorProviderId providerId = ConnectorProviderId.of(binding.provider());
        var provider = providerCatalog.requireProvider(providerId, binding.version());
        ConnectorConfigurationDocument document = new ConnectorConfigurationDocument(binding.config());
        providerCatalog.validateProviderConfiguration(
            providerId, binding.version(), document,
            "connector binding '" + binding.name() + "' provider " + providerId.value());
        Optional<ConnectorConfigSchemaDescriptor> schema = provider.provider().configurationSchema();
        return schema.map(value -> ConnectorConfigurationSnapshot.from(value, document, false).digest()).orElse("");
    }

    private static void requireCanonicalMatch(
        V3JavaTypeResolver resolver,
        String basePackage,
        String authored,
        String provided,
        String direction,
        PipelineYamlCallable callable
    ) {
        ClassName authoredClass = resolveJavaType(resolver, basePackage, authored);
        ClassName providedClass = resolveJavaType(resolver, basePackage, provided);
        Optional<String> authoredIdentity = resolver.semanticType(authoredClass);
        Optional<String> providedIdentity = resolver.semanticType(providedClass);
        boolean matches = authoredIdentity.isPresent() && providedIdentity.isPresent()
            ? authoredIdentity.equals(providedIdentity)
            : authoredClass.equals(providedClass);
        if (!matches) {
            throw new IllegalArgumentException("callable " + direction + " contract for "
                + callable.using() + "/" + callable.operation() + " is '" + provided
                + "', not authored canonical type '" + authored + "'");
        }
    }

    private static ClassName resolveJavaType(V3JavaTypeResolver resolver, String basePackage, String semanticType) {
        Optional<ClassName> resolved = resolver.resolve(semanticType);
        if (resolved.isPresent()) {
            return resolved.orElseThrow();
        }
        String token = semanticType.startsWith("<") && semanticType.endsWith(">")
            ? semanticType.substring(1, semanticType.length() - 1)
            : semanticType;
        resolved = resolver.resolve(token);
        if (resolved.isPresent()) {
            return resolved.orElseThrow();
        }
        return token.contains(".") ? ClassName.bestGuess(token) : ClassName.get(basePackage + ".domain", token);
    }

    private static CommandPolicy commandPolicy(Map<String, Object> values) {
        Set<String> supported = Set.of(
            "requireRetryRedrive", "requireIdempotency", "requireReconciliation",
            "requiredExecutionPosture", "minimumMachineConfirmation", "requireUserConfirmation");
        values.keySet().stream().filter(key -> !supported.contains(key)).sorted().findFirst().ifPresent(key -> {
            throw new IllegalArgumentException("callable command policy has unsupported field '" + key + "'");
        });
        return new CommandPolicy(
            bool(values, "requireRetryRedrive"), bool(values, "requireIdempotency"),
            bool(values, "requireReconciliation"),
            optionalEnum(values, "requiredExecutionPosture", CommandExecutionPosture.class),
            optionalEnum(values, "minimumMachineConfirmation", CommandMachineConfirmation.class),
            bool(values, "requireUserConfirmation"));
    }

    private static boolean bool(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean result) {
            return result;
        }
        throw new IllegalArgumentException("callable command policy " + key + " must be boolean");
    }

    private static <E extends Enum<E>> Optional<E> optionalEnum(
        Map<String, Object> values,
        String key,
        Class<E> type
    ) {
        Object value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (!(value instanceof String token) || token.isBlank()) {
            throw new IllegalArgumentException("callable command policy " + key + " must be a non-blank string");
        }
        try {
            return Optional.of(Enum.valueOf(type, token.trim().toUpperCase(java.util.Locale.ROOT)));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("callable command policy " + key
                + " has unsupported value '" + token + "'", failure);
        }
    }

    private static PipelineYamlStep requireStep(List<PipelineYamlStep> steps, String name, String subject) {
        return steps.stream().filter(step -> name.equals(step.name())).findFirst()
            .orElseThrow(() -> new IllegalArgumentException(subject + " step '" + name + "' does not exist"));
    }
}
