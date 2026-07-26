package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.StandardLocation;

import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;

/**
 * Validates checkpoint publication/subscription declarations loaded from pipeline YAML.
 */
final class CheckpointBoundaryValidator {

    private static final String QUEUE_ASYNC = "QUEUE_ASYNC";
    private static final String MAPPER_INTERFACE = "org.pipelineframework.mapper.Mapper";
    private static final String OBJECT_SNAPSHOT_MAPPER_INTERFACE = "org.pipelineframework.objectingest.ObjectSnapshotMapper";
    private static final String OBJECT_PUBLISH_MAPPER_INTERFACE = "org.pipelineframework.objectpublish.ObjectPublishMapper";
    private static final String STREAMING_OBJECT_PUBLISH_MAPPER_INTERFACE =
        "org.pipelineframework.objectpublish.StreamingObjectPublishMapper";

    void validate(
        PipelineTemplateConfig templateConfig,
        Path moduleDir,
        ProcessingEnvironment processingEnv,
        Messager messager
    ) {
        if (templateConfig == null) {
            return;
        }
        List<PipelineTemplateStep> steps = templateConfig.steps() == null ? List.of() : templateConfig.steps();
        boolean hasInputSubscription = templateConfig.input() != null && templateConfig.input().subscription() != null;
        boolean hasObjectInput = templateConfig.input() != null && templateConfig.input().object() != null;
        boolean hasOutputCheckpoint = templateConfig.output() != null && templateConfig.output().checkpoint() != null;
        boolean hasObjectOutput = templateConfig.output() != null && templateConfig.output().object() != null;
        if (!hasInputSubscription && !hasObjectInput && !hasOutputCheckpoint && !hasObjectOutput) {
            return;
        }
        if (templateConfig.platform() != null && templateConfig.platform().name().equals("FUNCTION")) {
            throw new IllegalStateException("Checkpoint publication/subscription is not supported on FUNCTION pipelines");
        }
        if (steps.isEmpty()) {
            throw new IllegalStateException("Checkpoint publication/subscription requires at least one pipeline step");
        }
        if (hasInputSubscription || hasOutputCheckpoint) {
            String orchestratorMode = loadOrchestratorMode(moduleDir, processingEnv);
            if (!QUEUE_ASYNC.equalsIgnoreCase(orchestratorMode)) {
                throw new IllegalStateException(
                    "Checkpoint publication/subscription requires pipeline.orchestrator.mode=QUEUE_ASYNC");
            }
        }
        if (hasInputSubscription) {
            validateSubscriptionMapper(templateConfig, processingEnv);
        }
        if (hasObjectInput) {
            validateObjectInput(templateConfig, processingEnv);
        }
        if (hasObjectOutput) {
            validateObjectOutput(templateConfig, processingEnv);
        }
        String publication = hasOutputCheckpoint ? templateConfig.output().checkpoint().publication() : null;
        if (hasOutputCheckpoint && (publication == null || publication.isBlank())) {
            throw new IllegalStateException("output.checkpoint.publication must not be blank");
        }
        if (messager != null && hasOutputCheckpoint) {
            messager.printMessage(Diagnostic.Kind.NOTE,
                "Checkpoint publication enabled for publication '" + publication + "'");
        }
    }

    private void validateObjectOutput(PipelineTemplateConfig templateConfig, ProcessingEnvironment processingEnv) {
        PipelineObjectOutputConfig objectOutput = templateConfig.output().object();
        Map<String, ?> publish = templateConfig.publish() == null ? Map.of() : templateConfig.publish();
        if (!publish.containsKey(objectOutput.target())) {
            throw new IllegalStateException("output object publish target not found: " + objectOutput.target());
        }
        String lastStepOutput = templateConfig.steps().getLast().outputTypeName();
        String expected = objectOutput.typeName() == null ? objectOutput.type() : objectOutput.typeName();
        if (expected == null || expected.isBlank()) {
            throw new IllegalStateException("Object output must declare a non-blank type or typeName");
        }
        if (lastStepOutput != null && !lastStepOutput.isBlank() && !typeMatches(lastStepOutput, expected)) {
            throw new IllegalStateException(
                "Object output consumes type '" + expected + "' must match last step output '" + lastStepOutput + "'");
        }
        validateObjectOutputMapper(objectOutput, expected, processingEnv);
        if (templateConfig.dialect() != org.pipelineframework.config.template.PipelineTemplateDialect.V3) {
            validateTerminalOutputMapper(templateConfig, expected, processingEnv);
        }
    }

    private void validateObjectOutputMapper(
        PipelineObjectOutputConfig objectOutput,
        String expected,
        ProcessingEnvironment processingEnv
    ) {
        if (processingEnv == null) {
            return;
        }
        String mapperClass = objectOutput.mapper();
        if (mapperClass == null || mapperClass.isBlank()) {
            return;
        }
        TypeElement mapperElement = processingEnv.getElementUtils().getTypeElement(mapperClass);
        if (mapperElement == null) {
            throw new IllegalStateException("Object output mapper type not found: " + mapperClass);
        }
        Types types = processingEnv.getTypeUtils();
        Optional<DeclaredType> mapperType = findImplementedInterface(mapperElement, OBJECT_PUBLISH_MAPPER_INTERFACE, processingEnv)
            .or(() -> findImplementedInterface(mapperElement, STREAMING_OBJECT_PUBLISH_MAPPER_INTERFACE, processingEnv));
        if (mapperType.isEmpty()) {
            throw new IllegalStateException(
                "Object output mapper '" + mapperClass + "' must implement "
                    + OBJECT_PUBLISH_MAPPER_INTERFACE + " or " + STREAMING_OBJECT_PUBLISH_MAPPER_INTERFACE);
        }
        DeclaredType declared = mapperType.get();
        if (declared.getTypeArguments().size() != 1) {
            throw new IllegalStateException(
                "Object output mapper '" + mapperClass
                    + "' must declare exactly one type argument for ObjectPublishMapper<PipelineOutput>");
        }
        String mappedType = declared.getTypeArguments().getFirst().toString();
        if (!typeMatches(expected, mappedType)) {
            throw new IllegalStateException(
                "Object output mapper '" + mapperClass + "' must declare ObjectPublishMapper<"
                    + expected + ">");
        }
    }

    private void validateTerminalOutputMapper(
        PipelineTemplateConfig templateConfig,
        String expected,
        ProcessingEnvironment processingEnv
    ) {
        if (processingEnv == null || "LOCAL".equalsIgnoreCase(templateConfig.transport())) {
            return;
        }
        PipelineTemplateStep terminalStep = templateConfig.steps().getLast();
        String outboundMapper = terminalStep.outboundMapper();
        if (outboundMapper == null || outboundMapper.isBlank()) {
            throw new IllegalStateException(
                "Object output requires terminal step outboundMapper to adapt transport output back to " + expected);
        }
        TypeElement mapperElement = processingEnv.getElementUtils().getTypeElement(outboundMapper);
        if (mapperElement == null) {
            throw new IllegalStateException("Terminal output mapper type not found: " + outboundMapper);
        }
        Optional<DeclaredType> mapperType = findImplementedInterface(mapperElement, MAPPER_INTERFACE, processingEnv);
        if (mapperType.isEmpty()) {
            throw new IllegalStateException(
                "Terminal output mapper '" + outboundMapper + "' must implement " + MAPPER_INTERFACE);
        }
        DeclaredType declared = mapperType.get();
        if (declared.getTypeArguments().size() != 2) {
            throw new IllegalStateException(
                "Terminal output mapper '" + outboundMapper
                    + "' must declare Mapper<PipelineOutput, ExternalOutput>");
        }
        String domainType = declared.getTypeArguments().getFirst().toString();
        if (!typeMatches(expected, domainType)) {
            throw new IllegalStateException(
                "Terminal output mapper '" + outboundMapper + "' must declare Mapper<"
                    + expected + ", ExternalOutput>");
        }
    }

    private Optional<DeclaredType> findImplementedInterface(
        TypeElement element,
        String interfaceName,
        ProcessingEnvironment processingEnv
    ) {
        TypeElement mapperInterface = processingEnv.getElementUtils().getTypeElement(interfaceName);
        if (mapperInterface == null) {
            throw new IllegalStateException("Mapper interface not found: " + interfaceName);
        }
        TypeMirror rawMapperInterfaceType = mapperInterface.asType();
        if (!(rawMapperInterfaceType instanceof DeclaredType mapperInterfaceType)) {
            throw new IllegalStateException("Mapper interface not resolvable as declared type: " + interfaceName);
        }
        return findImplementedInterface(
            element.asType(),
            mapperInterfaceType,
            processingEnv.getTypeUtils(),
            new HashSet<>());
    }

    private void validateObjectInput(PipelineTemplateConfig templateConfig, ProcessingEnvironment processingEnv) {
        PipelineObjectInputConfig objectInput = templateConfig.input().object();
        Map<String, ?> sources = templateConfig.sources() == null ? Map.of() : templateConfig.sources();
        if (!sources.containsKey(objectInput.source())) {
            throw new IllegalStateException("input object source not found: " + objectInput.source());
        }
        String firstStepInput = templateConfig.steps().getFirst().inputTypeName();
        String expected = objectInput.typeName() == null ? objectInput.type() : objectInput.typeName();
        if (expected == null || expected.isBlank()) {
            throw new IllegalStateException("Object input must declare a non-blank type or typeName");
        }
        if (firstStepInput != null && !firstStepInput.isBlank() && !typeMatches(firstStepInput, expected)) {
            throw new IllegalStateException(
                "Object input emits type '" + expected + "' must match first step input '" + firstStepInput + "'");
        }
        validateObjectInputMapper(objectInput, expected, processingEnv);
    }

    private void validateObjectInputMapper(
        PipelineObjectInputConfig objectInput,
        String expected,
        ProcessingEnvironment processingEnv
    ) {
        if (processingEnv == null) {
            return;
        }
        String mapperClass = objectInput.mapper();
        if (mapperClass == null || mapperClass.isBlank()) {
            return;
        }
        TypeElement mapperElement = processingEnv.getElementUtils().getTypeElement(mapperClass);
        if (mapperElement == null) {
            throw new IllegalStateException("Object input mapper type not found: " + mapperClass);
        }
        TypeElement mapperInterface = processingEnv.getElementUtils().getTypeElement(OBJECT_SNAPSHOT_MAPPER_INTERFACE);
        if (mapperInterface == null) {
            throw new IllegalStateException("ObjectSnapshotMapper interface not found: " + OBJECT_SNAPSHOT_MAPPER_INTERFACE);
        }
        TypeMirror rawMapperInterfaceType = mapperInterface.asType();
        if (!(rawMapperInterfaceType instanceof DeclaredType mapperInterfaceType)) {
            throw new IllegalStateException(
                "ObjectSnapshotMapper interface not resolvable as declared type: " + OBJECT_SNAPSHOT_MAPPER_INTERFACE);
        }
        Types types = processingEnv.getTypeUtils();
        Optional<DeclaredType> mapperType = findImplementedInterface(
            mapperElement.asType(), mapperInterfaceType, types, new HashSet<>());
        if (mapperType.isEmpty()) {
            throw new IllegalStateException(
                "Object input mapper '" + mapperClass + "' must implement "
                    + OBJECT_SNAPSHOT_MAPPER_INTERFACE);
        }
        DeclaredType declared = mapperType.get();
        if (declared.getTypeArguments().size() != 1) {
            throw new IllegalStateException(
                "Object input mapper '" + mapperClass
                    + "' must declare exactly one type argument for ObjectSnapshotMapper<PipelineInput>");
        }
        String mappedType = declared.getTypeArguments().getFirst().toString();
        if (!typeMatches(expected, mappedType)) {
            throw new IllegalStateException(
                "Object input mapper '" + mapperClass + "' must declare ObjectSnapshotMapper<"
                    + expected + ">");
        }
    }

    private Optional<DeclaredType> findImplementedInterface(
        TypeMirror type,
        DeclaredType targetInterface,
        Types types,
        Set<String> visited
    ) {
        if (!(type instanceof DeclaredType declared) || !visited.add(types.erasure(type).toString())) {
            return Optional.empty();
        }
        if (types.isSameType(types.erasure(declared), types.erasure(targetInterface))) {
            return Optional.of(declared);
        }
        for (TypeMirror supertype : types.directSupertypes(declared)) {
            Optional<DeclaredType> match = findImplementedInterface(supertype, targetInterface, types, visited);
            if (match.isPresent()) {
                return match;
            }
        }
        return Optional.empty();
    }

    private void validateSubscriptionMapper(PipelineTemplateConfig templateConfig, ProcessingEnvironment processingEnv) {
        if (templateConfig.input() == null || templateConfig.input().subscription() == null) {
            return;
        }
        String mapperClass = templateConfig.input().subscription().mapper();
        if (mapperClass == null || mapperClass.isBlank() || processingEnv == null) {
            return;
        }
        TypeElement mapperElement = processingEnv.getElementUtils().getTypeElement(mapperClass);
        if (mapperElement == null) {
            throw new IllegalStateException("Subscription mapper type not found: " + mapperClass);
        }
        TypeElement mapperInterface = processingEnv.getElementUtils().getTypeElement(MAPPER_INTERFACE);
        if (mapperInterface == null) {
            throw new IllegalStateException("Mapper interface not found: " + MAPPER_INTERFACE);
        }
        TypeMirror rawMapperInterfaceType = mapperInterface.asType();
        if (!(rawMapperInterfaceType instanceof DeclaredType mapperInterfaceType)) {
            throw new IllegalStateException("Mapper interface not resolvable as declared type: " + MAPPER_INTERFACE);
        }
        Types types = processingEnv.getTypeUtils();
        boolean implementsMapper = mapperElement.getInterfaces().stream()
            .filter(DeclaredType.class::isInstance)
            .map(DeclaredType.class::cast)
            .anyMatch(type -> Objects.equals(
                processingEnv.getTypeUtils().erasure(type),
                processingEnv.getTypeUtils().erasure(mapperInterfaceType)));
        if (!implementsMapper) {
            throw new IllegalStateException(
                "Subscription mapper '" + mapperClass + "' must implement " + MAPPER_INTERFACE);
        }

        String inputTypeName = templateConfig.steps().getFirst().inputTypeName();
        for (TypeMirror implemented : mapperElement.getInterfaces()) {
            if (!(implemented instanceof DeclaredType declared)
                || !types.isSameType(types.erasure(declared), types.erasure(mapperInterfaceType))) {
                continue;
            }
            if (declared.getTypeArguments().size() != 2) {
                throw new IllegalStateException(
                    "Subscription mapper '" + mapperClass
                        + "' must declare exactly two type arguments for Mapper<CheckpointPayload, PipelineInput>");
            }
            String domainType = declared.getTypeArguments().getFirst().toString();
            if (inputTypeName != null
                && !inputTypeName.isBlank()
                && !typeMatches(inputTypeName, domainType)) {
                throw new IllegalStateException(
                    "Subscription mapper '" + mapperClass + "' must declare Mapper<"
                        + inputTypeName
                        + ", PipelineInput>");
            }
        }
    }

    private boolean typeMatches(String expectedTypeName, String actualTypeName) {
        if (expectedTypeName.equals(actualTypeName)) {
            return true;
        }
        if (expectedTypeName.contains(".") && actualTypeName.contains(".")) {
            return false;
        }
        return simpleTypeName(expectedTypeName).equals(simpleTypeName(actualTypeName));
    }

    private String simpleTypeName(String typeName) {
        if (typeName == null || typeName.isBlank()) {
            return "";
        }
        int lastDot = typeName.lastIndexOf('.');
        return lastDot >= 0 ? typeName.substring(lastDot + 1) : typeName;
    }

    private String loadOrchestratorMode(Path moduleDir, ProcessingEnvironment processingEnv) {
        Properties properties = new Properties();
        if (moduleDir != null) {
            Path applicationProperties = moduleDir.resolve(Path.of("src", "main", "resources", "application.properties"));
            if (Files.exists(applicationProperties)) {
                try (InputStream inputStream = Files.newInputStream(applicationProperties)) {
                    properties.load(inputStream);
                    return properties.getProperty("pipeline.orchestrator.mode");
                } catch (IOException e) {
                    throw new IllegalStateException("Failed reading application.properties for checkpoint validation", e);
                }
            }
        }
        if (processingEnv == null) {
            return null;
        }
        try (InputStream inputStream = processingEnv.getFiler()
            .getResource(StandardLocation.SOURCE_PATH, "", "application.properties")
            .openInputStream()) {
            properties.load(inputStream);
            return properties.getProperty("pipeline.orchestrator.mode");
        } catch (Exception ignored) {
            return null;
        }
    }
}
