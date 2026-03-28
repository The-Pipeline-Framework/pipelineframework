package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
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

/**
 * Validates checkpoint publication/subscription declarations loaded from pipeline YAML.
 */
final class CheckpointBoundaryValidator {

    private static final String QUEUE_ASYNC = "QUEUE_ASYNC";
    private static final String MAPPER_INTERFACE = "org.pipelineframework.mapper.Mapper";

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
        boolean hasOutputCheckpoint = templateConfig.output() != null && templateConfig.output().checkpoint() != null;
        if (!hasInputSubscription && !hasOutputCheckpoint) {
            return;
        }
        if (templateConfig.platform() != null && templateConfig.platform().name().equals("FUNCTION")) {
            throw new IllegalStateException("Checkpoint publication/subscription is not supported on FUNCTION pipelines");
        }
        if (steps.isEmpty()) {
            throw new IllegalStateException("Checkpoint publication/subscription requires at least one pipeline step");
        }
        String orchestratorMode = loadOrchestratorMode(moduleDir, processingEnv);
        if (!QUEUE_ASYNC.equalsIgnoreCase(orchestratorMode)) {
            throw new IllegalStateException(
                "Checkpoint publication/subscription requires pipeline.orchestrator.mode=QUEUE_ASYNC");
        }
        if (hasInputSubscription) {
            validateSubscriptionMapper(templateConfig, processingEnv);
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
                    "Subscription mapper '" + mapperClass + "' must declare exactly two type arguments for Mapper<PublishedCheckpoint, PipelineInput>");
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
