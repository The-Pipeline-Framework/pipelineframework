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

package org.pipelineframework.proto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.*;

/**
 * Generates protobuf definitions from the pipeline template configuration.
 */
public class PipelineProtoGenerator {

    private static final String ORCHESTRATOR_PROTO = "orchestrator.proto";

    /**
     * Creates a new PipelineProtoGenerator.
     */
    public PipelineProtoGenerator() {
    }

    /**
     * Command-line entry point for the generator.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        Arguments arguments = Arguments.parse(args);
        PipelineProtoGenerator generator = new PipelineProtoGenerator();
        generator.generate(arguments.moduleDir(), arguments.configPath(), arguments.outputDir());
    }

    /**
     * Generate proto definitions from the pipeline template configuration.
     *
     * @param moduleDir the module directory
     * @param configPath the pipeline template config path, or null to locate automatically
     * @param outputDir the output directory, or null to use the default
     */
    public void generate(Path moduleDir, Path configPath, Path outputDir) {
        Path resolvedModuleDir = moduleDir == null ? Path.of("") : moduleDir;
        Path resolvedConfig = resolveConfigPath(resolvedModuleDir, configPath);
        Path resolvedOutput = outputDir != null
            ? outputDir
            : resolvedModuleDir.resolve("target").resolve("generated-sources").resolve("proto");

        PipelineTemplateConfigLoader loader = new PipelineTemplateConfigLoader();
        PipelineTemplateConfig config = loader.load(resolvedConfig);
        if (config.basePackage() == null || config.basePackage().isBlank()) {
            throw new IllegalStateException("pipeline-config.yaml is missing basePackage");
        }
        List<PipelineTemplateStep> steps = config.steps();
        if (steps == null || steps.isEmpty()) {
            return;
        }

        List<ResolvedStep> resolvedSteps = normalizeSteps(steps);
        try {
            Files.createDirectories(resolvedOutput);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create proto output directory: " + resolvedOutput, e);
        }

        List<AspectDefinition> aspectDefinitions = toAspectDefinitions(config.aspects());

        for (int i = 0; i < resolvedSteps.size(); i++) {
            ResolvedStep step = resolvedSteps.get(i);
            ResolvedStep previous = i > 0 ? resolvedSteps.get(i - 1) : null;
            String content = renderStepProto(config.basePackage(), step, previous, i == 0, aspectDefinitions);
            Path protoPath = resolvedOutput.resolve(step.serviceName() + ".proto");
            writeProto(protoPath, content);
            System.out.println("[PipelineProtoGenerator] wrote " + protoPath.toAbsolutePath());
            if (!Files.isRegularFile(protoPath)) {
                System.out.println("[PipelineProtoGenerator] warning: expected proto file missing at " +
                    protoPath.toAbsolutePath());
            }
        }

        String transport = config.transport();
        if (transport == null || transport.isBlank() || "GRPC".equalsIgnoreCase(transport)) {
            String content = renderOrchestratorProto(config.basePackage(), resolvedSteps);
            Path protoPath = resolvedOutput.resolve(ORCHESTRATOR_PROTO);
            writeProto(protoPath, content);
            System.out.println("[PipelineProtoGenerator] wrote " + protoPath.toAbsolutePath());
            if (!Files.isRegularFile(protoPath)) {
                System.out.println("[PipelineProtoGenerator] warning: expected proto file missing at " +
                    protoPath.toAbsolutePath());
            }
        }
    }

    private Path resolveConfigPath(Path moduleDir, Path configPath) {
        if (configPath != null) {
            return configPath;
        }
        PipelineYamlConfigLocator locator = new PipelineYamlConfigLocator();
        Optional<Path> located = locator.locate(moduleDir);
        if (located.isEmpty()) {
            throw new IllegalStateException("No pipeline template config found from " + moduleDir.toAbsolutePath());
        }
        return located.get();
    }

    private void writeProto(Path outputPath, String content) {
        try {
            Files.writeString(outputPath, content);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write proto file: " + outputPath, e);
        }
    }

    private List<ResolvedStep> normalizeSteps(List<PipelineTemplateStep> steps) {
        List<ResolvedStep> resolved = new ArrayList<>();
        ResolvedStep previous = null;
        for (int i = 0; i < steps.size(); i++) {
            PipelineTemplateStep step = steps.get(i);
            String serviceName = toServiceName(step.name());
            String serviceNameFormatted = formatForClassName(stripProcessPrefix(step.name()));
            String inputTypeName = step.inputTypeName();
            List<PipelineTemplateField> inputFields = step.inputFields();
            if (i > 0 && previous != null) {
                inputTypeName = previous.outputTypeName();
                inputFields = copyFields(previous.outputFields());
            }
            ResolvedStep resolvedStep = new ResolvedStep(
                step.name(),
                serviceName,
                serviceNameFormatted,
                step.cardinality(),
                inputTypeName,
                inputFields,
                step.outputTypeName(),
                step.outputFields());
            resolved.add(resolvedStep);
            previous = resolvedStep;
        }
        return resolved;
    }

    private List<PipelineTemplateField> copyFields(List<PipelineTemplateField> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        List<PipelineTemplateField> copy = new ArrayList<>();
        for (PipelineTemplateField field : fields) {
            if (field != null) {
                copy.add(new PipelineTemplateField(field.name(), field.type(), field.protoType()));
            }
        }
        return copy;
    }

    private String renderStepProto(
        String basePackage,
        ResolvedStep step,
        ResolvedStep previous,
        boolean firstStep,
        List<AspectDefinition> aspects
    ) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");

        if (!firstStep && previous != null) {
            builder.append("import \"")
                .append(previous.serviceName())
                .append(".proto\";\n\n");
        }

        if (firstStep) {
            renderMessage(builder, step.inputTypeName(), step.inputFields(), 1);
            builder.append('\n');
        }

        int outputStartNumber = 1;
        List<PipelineTemplateField> inputFields = step.inputFields();
        if (inputFields != null && !inputFields.isEmpty()) {
            outputStartNumber = inputFields.size() + 1;
        }
        renderMessage(builder, step.outputTypeName(), step.outputFields(), outputStartNumber);
        builder.append('\n');

        renderService(builder, step, previous, firstStep);
        builder.append('\n');

        renderAspectServices(builder, step, firstStep, aspects);

        return builder.toString();
    }

    private void renderMessage(
        StringBuilder builder,
        String typeName,
        List<PipelineTemplateField> fields,
        int startNumber
    ) {
        builder.append("message ").append(typeName).append(" {\n");
        int index = 0;
        if (fields != null) {
            for (PipelineTemplateField field : fields) {
                if (field == null || field.name() == null || field.name().isBlank()) {
                    continue;
                }
                String fieldType = renderFieldType(field);
                int number = startNumber + index;
                builder.append("  ").append(fieldType).append(' ')
                    .append(field.name()).append(" = ")
                    .append(number).append(";\n");
                index++;
            }
        }
        builder.append("}\n");
    }

    private String renderFieldType(PipelineTemplateField field) {
        String javaType = field.type();
        if (isListType(javaType)) {
            return "repeated " + listInnerType(javaType);
        }
        if (isMapType(javaType)) {
            return "map<" + mapKeyType(javaType) + ", " + mapValueType(javaType) + ">";
        }
        String protoType = field.protoType();
        return protoType == null || protoType.isBlank() ? "string" : protoType;
    }

    private void renderService(
        StringBuilder builder,
        ResolvedStep step,
        ResolvedStep previous,
        boolean firstStep
    ) {
        builder.append("service Process")
            .append(step.serviceNameFormatted())
            .append("Service {\n");
        String inputType = firstStep ? step.inputTypeName() : previous.outputTypeName();
        String outputType = step.outputTypeName();
        String cardinality = step.cardinality();
        if ("EXPANSION".equalsIgnoreCase(cardinality)) {
            builder.append("  rpc remoteProcess(")
                .append(inputType)
                .append(") returns (stream ")
                .append(outputType)
                .append(");\n");
        } else if ("MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            builder.append("  rpc remoteProcess(stream ")
                .append(inputType)
                .append(") returns (stream ")
                .append(outputType)
                .append(");\n");
        } else if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            builder.append("  rpc remoteProcess(stream ")
                .append(inputType)
                .append(") returns (")
                .append(outputType)
                .append(");\n");
        } else {
            builder.append("  rpc remoteProcess(")
                .append(inputType)
                .append(") returns (")
                .append(outputType)
                .append(");\n");
        }
        builder.append("}\n");
    }

    private void renderAspectServices(
        StringBuilder builder,
        ResolvedStep step,
        boolean firstStep,
        List<AspectDefinition> aspects
    ) {
        if (aspects == null || aspects.isEmpty()) {
            return;
        }
        List<AspectDefinition> beforeAspects = new ArrayList<>();
        List<AspectDefinition> afterAspects = new ArrayList<>();
        for (AspectDefinition aspect : aspects) {
            if ("BEFORE_STEP".equalsIgnoreCase(aspect.position())) {
                beforeAspects.add(aspect);
            } else {
                afterAspects.add(aspect);
            }
        }

        if (firstStep) {
            for (AspectDefinition aspect : beforeAspects) {
                renderAspectService(builder, aspect, step.inputTypeName());
            }
            if (!beforeAspects.isEmpty()) {
                builder.append('\n');
            }
        }

        for (AspectDefinition aspect : afterAspects) {
            renderAspectService(builder, aspect, step.outputTypeName());
        }

        if (!afterAspects.isEmpty()) {
            builder.append('\n');
        }

        for (AspectDefinition aspect : beforeAspects) {
            renderAspectService(builder, aspect, step.outputTypeName());
        }
    }

    private void renderAspectService(StringBuilder builder, AspectDefinition aspect, String typeName) {
        builder.append("service ")
            .append(observeServiceName(aspect.name(), typeName))
            .append(" {\n");
        builder.append("  rpc remoteProcess(")
            .append(typeName)
            .append(") returns (")
            .append(typeName)
            .append(");\n");
        builder.append("}\n");
    }

    private String renderOrchestratorProto(String basePackage, List<ResolvedStep> steps) {
        if (steps == null || steps.isEmpty()) {
            return "";
        }
        ResolvedStep first = steps.get(0);
        ResolvedStep last = steps.get(steps.size() - 1);
        StreamingShape shape = computePipelineStreamingShape(steps);

        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");
        builder.append("import \"")
            .append(first.serviceName())
            .append(".proto\";\n");
        if (!first.serviceName().equals(last.serviceName())) {
            builder.append("import \"")
                .append(last.serviceName())
                .append(".proto\";\n");
        }
        builder.append('\n');
        builder.append("service OrchestratorService {\n");
        builder.append("  rpc Run (");
        if (shape.inputStreaming()) {
            builder.append("stream ");
        }
        builder.append(first.inputTypeName());
        builder.append(") returns (");
        if (shape.outputStreaming()) {
            builder.append("stream ");
        }
        builder.append(last.outputTypeName());
        builder.append(");\n");
        builder.append("}\n");
        return builder.toString();
    }

    private StreamingShape computePipelineStreamingShape(List<ResolvedStep> steps) {
        boolean inputStreaming = false;
        if (steps != null && !steps.isEmpty()) {
            inputStreaming = isStreamingInputCardinality(steps.get(0).cardinality());
        }
        boolean outputStreaming = inputStreaming;
        if (steps != null) {
            for (ResolvedStep step : steps) {
                outputStreaming = applyCardinalityToStreaming(step.cardinality(), outputStreaming);
            }
        }
        return new StreamingShape(inputStreaming, outputStreaming);
    }

    private boolean isStreamingInputCardinality(String cardinality) {
        return "REDUCTION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality);
    }

    private boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        if ("EXPANSION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return true;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return false;
        }
        return currentStreaming;
    }

    private List<AspectDefinition> toAspectDefinitions(Map<String, PipelineTemplateAspect> aspects) {
        if (aspects == null || aspects.isEmpty()) {
            return List.of();
        }
        List<AspectDefinition> definitions = new ArrayList<>();
        for (Map.Entry<String, PipelineTemplateAspect> entry : aspects.entrySet()) {
            String name = entry.getKey();
            if (name == null || name.isBlank()) {
                continue;
            }
            PipelineTemplateAspect aspect = entry.getValue();
            if (aspect == null || !aspect.enabled()) {
                continue;
            }
            String position = aspect.position();
            if (position == null || position.isBlank()) {
                position = "AFTER_STEP";
            }
            List<String> enabledTargets = readEnabledTargets(aspect.config());
            if (enabledTargets.isEmpty()) {
                continue;
            }
            if (!enabledTargets.contains("CLIENT_STEP") && !enabledTargets.contains("GRPC_SERVICE")) {
                continue;
            }
            definitions.add(new AspectDefinition(name, position, enabledTargets));
        }
        return definitions;
    }

    private List<String> readEnabledTargets(Map<String, Object> config) {
        if (config == null) {
            return List.of();
        }
        Object targetsObj = config.get("enabledTargets");
        if (!(targetsObj instanceof Iterable<?> targets)) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (Object target : targets) {
            if (target != null) {
                String value = target.toString();
                if (!value.isBlank()) {
                    values.add(value);
                }
            }
        }
        return values;
    }

    private String toServiceName(String name) {
        if (name == null || name.isBlank()) {
            return "";
        }
        String replaced = name.replaceAll("[^A-Za-z0-9]", "-").toLowerCase(Locale.ROOT);
        String collapsed = replaced.replaceAll("-+", "-");
        if (collapsed.startsWith("-")) {
            collapsed = collapsed.substring(1);
        }
        if (collapsed.endsWith("-")) {
            collapsed = collapsed.substring(0, collapsed.length() - 1);
        }
        return collapsed + "-svc";
    }

    private String formatForClassName(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        String[] parts = input.split(" ");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(Locale.ROOT);
            builder.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                builder.append(lower.substring(1));
            }
        }
        return builder.toString();
    }

    private String stripProcessPrefix(String name) {
        if (name == null) {
            return "";
        }
        if (name.startsWith("Process ")) {
            return name.substring("Process ".length());
        }
        return name;
    }

    private boolean isListType(String type) {
        return type != null && type.startsWith("List<") && type.endsWith(">");
    }

    private String listInnerType(String type) {
        if (!isListType(type)) {
            return type;
        }
        return type.substring(5, type.length() - 1).trim();
    }

    private boolean isMapType(String type) {
        if (type == null || !type.startsWith("Map<") || !type.endsWith(">")) {
            return false;
        }
        String innerContent = type.substring(4, type.length() - 1);
        return innerContent.contains(",");
    }

    private String mapKeyType(String type) {
        if (!isMapType(type)) {
            return "string";
        }
        String innerContent = type.substring(4, type.length() - 1);
        String[] parts = innerContent.split(",");
        String keyType = parts.length > 0 ? parts[0].trim() : "string";
        return toProtoScalar(keyType, true);
    }

    private String mapValueType(String type) {
        if (!isMapType(type)) {
            return "string";
        }
        String innerContent = type.substring(4, type.length() - 1);
        String[] parts = innerContent.split(",");
        String valueType = parts.length > 1 ? parts[1].trim() : "string";
        return toProtoScalar(valueType, false);
    }

    private String toProtoScalar(String javaType, boolean keyType) {
        if (javaType == null || javaType.isBlank()) {
            return "string";
        }
        return switch (javaType) {
            case "String", "UUID", "BigDecimal", "Currency", "Path", "LocalDateTime", "LocalDate", "OffsetDateTime",
                "ZonedDateTime", "Instant", "Duration", "Period", "URI", "URL", "File", "BigInteger" -> "string";
            case "Integer", "AtomicInteger" -> "int32";
            case "Long", "AtomicLong" -> "int64";
            case "Double" -> "double";
            case "Float" -> "float";
            case "Boolean" -> "bool";
            case "List<String>" -> "string";
            default -> keyType ? "string" : javaType;
        };
    }

    private String observeServiceName(String aspectName, String typeName) {
        if (aspectName == null || aspectName.isBlank() || typeName == null || typeName.isBlank()) {
            return "";
        }
        String[] parts = aspectName.trim().split("[^A-Za-z0-9]+");
        StringBuilder aspectPascal = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(Locale.ROOT);
            aspectPascal.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                aspectPascal.append(lower.substring(1));
            }
        }
        return "Observe" + aspectPascal + typeName.trim() + "SideEffectService";
    }

    private record ResolvedStep(
        String name,
        String serviceName,
        String serviceNameFormatted,
        String cardinality,
        String inputTypeName,
        List<PipelineTemplateField> inputFields,
        String outputTypeName,
        List<PipelineTemplateField> outputFields
    ) {
    }

    private record StreamingShape(boolean inputStreaming, boolean outputStreaming) {
    }

    private record AspectDefinition(String name, String position, List<String> enabledTargets) {
    }

    private record Arguments(Path moduleDir, Path configPath, Path outputDir) {
        static Arguments parse(String[] args) {
            Path moduleDir = null;
            Path configPath = null;
            Path outputDir = null;
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    String arg = args[i];
                    if (arg.startsWith("--module-dir=")) {
                        moduleDir = Path.of(arg.substring("--module-dir=".length()));
                    } else if ("--module-dir".equals(arg) && i + 1 < args.length) {
                        moduleDir = Path.of(args[++i]);
                    } else if (arg.startsWith("--config=")) {
                        configPath = Path.of(arg.substring("--config=".length()));
                    } else if ("--config".equals(arg) && i + 1 < args.length) {
                        configPath = Path.of(args[++i]);
                    } else if (arg.startsWith("--output-dir=")) {
                        outputDir = Path.of(arg.substring("--output-dir=".length()));
                    } else if ("--output-dir".equals(arg) && i + 1 < args.length) {
                        outputDir = Path.of(args[++i]);
                    } else if ("--help".equals(arg) || "-h".equals(arg)) {
                        printUsage();
                        System.exit(0);
                    }
                }
            }
            return new Arguments(moduleDir, configPath, outputDir);
        }

        static void printUsage() {
            System.out.println("Usage: PipelineProtoGenerator [--module-dir DIR] [--config PATH] [--output-dir DIR]");
        }
    }
}
