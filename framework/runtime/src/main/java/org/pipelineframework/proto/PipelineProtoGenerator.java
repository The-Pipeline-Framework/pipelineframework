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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.PipelineIdlCompatibilityChecker;
import org.pipelineframework.config.template.PipelineIdlSnapshot;
import org.pipelineframework.config.template.PipelineTemplateAspect;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateField;
import org.pipelineframework.config.template.PipelineTemplateMessage;
import org.pipelineframework.config.template.PipelineTemplateStep;

/**
 * Generates protobuf definitions from the pipeline template configuration.
 */
public class PipelineProtoGenerator {

    private static final String ORCHESTRATOR_PROTO = "orchestrator.proto";
    private static final String TYPES_PROTO = "pipeline-types.proto";
    private static final String IDL_SNAPSHOT_PROPERTY = "tpf.idl.compat.baseline";
    private static final String IDL_SNAPSHOT_ENV = "TPF_IDL_COMPAT_BASELINE";
    private static final ObjectMapper IDL_MAPPER = PipelineJson.mapper().findAndRegisterModules();

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
        try {
            Files.createDirectories(resolvedOutput);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create proto output directory: " + resolvedOutput, e);
        }

        writeIdlSnapshot(resolvedModuleDir, config);

        List<PipelineTemplateStep> steps = config.steps();
        if (steps == null || steps.isEmpty()) {
            return;
        }

        boolean v2 = config.version() >= 2;
        List<ResolvedStep> resolvedSteps = normalizeSteps(steps);
        List<AspectDefinition> aspectDefinitions = toAspectDefinitions(config.aspects());

        if (v2) {
            Path typesProtoPath = resolvedOutput.resolve(TYPES_PROTO);
            writeProto(typesProtoPath, renderTypesProto(config.basePackage(), config.messages()));
        }

        for (int i = 0; i < resolvedSteps.size(); i++) {
            ResolvedStep step = resolvedSteps.get(i);
            ResolvedStep previous = i > 0 ? resolvedSteps.get(i - 1) : null;
            String content = renderStepProto(config.basePackage(), step, previous, i == 0, aspectDefinitions, v2);
            Path protoPath = resolvedOutput.resolve(step.serviceName() + ".proto");
            writeProto(protoPath, content);
        }

        String transport = config.transport();
        if (transport == null || transport.isBlank() || "GRPC".equalsIgnoreCase(transport)) {
            String content = renderOrchestratorProto(config.basePackage(), resolvedSteps, v2);
            Path protoPath = resolvedOutput.resolve(ORCHESTRATOR_PROTO);
            writeProto(protoPath, content);
        }
    }

    private void writeIdlSnapshot(Path moduleDir, PipelineTemplateConfig config) {
        PipelineIdlSnapshot snapshot = PipelineIdlSnapshot.from(config);
        Path outputPath = moduleDir.resolve("target")
            .resolve("generated-resources")
            .resolve("META-INF")
            .resolve("pipeline")
            .resolve("idl.json");
        try {
            Files.createDirectories(outputPath.getParent());
            IDL_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputPath.toFile(), snapshot);
            String baseline = resolveCompatibilityBaseline();
            if (baseline != null && !baseline.isBlank()) {
                PipelineIdlSnapshot baselineSnapshot = IDL_MAPPER.readValue(Path.of(baseline).toFile(), PipelineIdlSnapshot.class);
                List<String> errors = new PipelineIdlCompatibilityChecker().compare(baselineSnapshot, snapshot);
                if (!errors.isEmpty()) {
                    throw new IllegalStateException("IDL compatibility check failed:\n - " + String.join("\n - ", errors));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write IDL snapshot", e);
        }
    }

    private String resolveCompatibilityBaseline() {
        String value = System.getProperty(IDL_SNAPSHOT_PROPERTY);
        if (value == null || value.isBlank()) {
            value = System.getenv(IDL_SNAPSHOT_ENV);
        }
        return value == null || value.isBlank() ? null : value.trim();
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
        return List.copyOf(fields);
    }

    private String renderTypesProto(String basePackage, Map<String, PipelineTemplateMessage> messages) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");
        boolean first = true;
        for (PipelineTemplateMessage message : messages.values()) {
            if (!first) {
                builder.append('\n');
            }
            renderMessage(builder, message);
            first = false;
        }
        return builder.toString();
    }

    private String renderStepProto(
        String basePackage,
        ResolvedStep step,
        ResolvedStep previous,
        boolean firstStep,
        List<AspectDefinition> aspects,
        boolean v2
    ) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");

        if (v2) {
            builder.append("import \"").append(TYPES_PROTO).append("\";\n\n");
        } else if (!firstStep && previous != null) {
            builder.append("import \"")
                .append(previous.serviceName())
                .append(".proto\";\n\n");
        }

        if (!v2 && firstStep) {
            renderLegacyMessage(builder, step.inputTypeName(), step.inputFields(), 1);
            builder.append('\n');
        }

        if (!v2) {
            int outputStartNumber = 1;
            List<PipelineTemplateField> inputFields = step.inputFields();
            if (inputFields != null && !inputFields.isEmpty()) {
                outputStartNumber = inputFields.size() + 1;
            }
            renderLegacyMessage(builder, step.outputTypeName(), step.outputFields(), outputStartNumber);
            builder.append('\n');
        }

        renderService(builder, step, previous, firstStep);
        builder.append('\n');
        renderAspectServices(builder, step, firstStep, aspects);
        return builder.toString();
    }

    private void renderMessage(StringBuilder builder, PipelineTemplateMessage message) {
        builder.append("message ").append(message.name()).append(" {\n");
        if (!message.reserved().numbers().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < message.reserved().numbers().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(message.reserved().numbers().get(i));
            }
            builder.append(";\n");
        }
        if (!message.reserved().names().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < message.reserved().names().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append('"').append(message.reserved().names().get(i)).append('"');
            }
            builder.append(";\n");
        }
        for (PipelineTemplateField field : message.fields()) {
            renderFieldLine(builder, field);
        }
        builder.append("}\n");
    }

    private void renderLegacyMessage(
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
                String fieldType = renderLegacyFieldType(field);
                int number = startNumber + index;
                builder.append("  ").append(fieldType).append(' ')
                    .append(field.name()).append(" = ")
                    .append(number).append(";\n");
                index++;
            }
        }
        builder.append("}\n");
    }

    private void renderFieldLine(StringBuilder builder, PipelineTemplateField field) {
        if (field.comment() != null && !field.comment().isBlank()) {
            builder.append("  // ").append(field.comment()).append('\n');
        }
        builder.append("  ");
        if (field.repeated()) {
            builder.append("repeated ");
        } else if (field.optional() && supportsOptionalLabel(field)) {
            builder.append("optional ");
        }
        builder.append(field.protoType())
            .append(' ')
            .append(field.name())
            .append(" = ")
            .append(field.number());
        if (field.deprecated()) {
            builder.append(" [deprecated = true]");
        }
        builder.append(";\n");
    }

    private boolean supportsOptionalLabel(PipelineTemplateField field) {
        return !field.isMap() && !field.isMessageReference();
    }

    private String renderLegacyFieldType(PipelineTemplateField field) {
        if (field.repeated()) {
            return "repeated " + field.protoType();
        }
        return field.protoType() == null || field.protoType().isBlank() ? "string" : field.protoType();
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
        CardinalitySemantics canonicalCardinality = CardinalitySemantics.fromString(step.cardinality());
        if (canonicalCardinality == CardinalitySemantics.ONE_TO_MANY) {
            builder.append("  rpc remoteProcess(")
                .append(inputType)
                .append(") returns (stream ")
                .append(outputType)
                .append(");\n");
        } else if (canonicalCardinality == CardinalitySemantics.MANY_TO_MANY) {
            builder.append("  rpc remoteProcess(stream ")
                .append(inputType)
                .append(") returns (stream ")
                .append(outputType)
                .append(");\n");
        } else if (canonicalCardinality == CardinalitySemantics.MANY_TO_ONE) {
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

    private String renderOrchestratorProto(String basePackage, List<ResolvedStep> steps, boolean v2) {
        if (steps == null || steps.isEmpty()) {
            return "";
        }
        ResolvedStep first = steps.get(0);
        ResolvedStep last = steps.get(steps.size() - 1);
        StreamingShape shape = computePipelineStreamingShape(steps);

        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");
        if (v2) {
            builder.append("import \"").append(TYPES_PROTO).append("\";\n");
        } else {
            builder.append("import \"")
                .append(first.serviceName())
                .append(".proto\";\n");
            if (!first.serviceName().equals(last.serviceName())) {
                builder.append("import \"")
                    .append(last.serviceName())
                    .append(".proto\";\n");
            }
        }
        builder.append("import \"google/protobuf/empty.proto\";\n");
        builder.append('\n');
        builder.append("message RunAsyncRequest {\n");
        builder.append("  ").append(first.inputTypeName()).append(" input = 1;\n");
        builder.append("  repeated ").append(first.inputTypeName()).append(" input_batch = 2;\n");
        builder.append("  string tenant_id = 3;\n");
        builder.append("  string idempotency_key = 4;\n");
        builder.append("}\n\n");
        builder.append("message RunAsyncResponse {\n");
        builder.append("  string execution_id = 1;\n");
        builder.append("  bool duplicate = 2;\n");
        builder.append("  string status_url = 3;\n");
        builder.append("  int64 accepted_at_epoch_ms = 4;\n");
        builder.append("}\n\n");
        builder.append("message GetExecutionStatusRequest {\n");
        builder.append("  string tenant_id = 1;\n");
        builder.append("  string execution_id = 2;\n");
        builder.append("}\n\n");
        builder.append("message GetExecutionStatusResponse {\n");
        builder.append("  string execution_id = 1;\n");
        builder.append("  string status = 2;\n");
        builder.append("  int32 current_step_index = 3;\n");
        builder.append("  int32 attempt = 4;\n");
        builder.append("  int64 version = 5;\n");
        builder.append("  int64 next_due_epoch_ms = 6;\n");
        builder.append("  int64 updated_at_epoch_ms = 7;\n");
        builder.append("  string error_code = 8;\n");
        builder.append("  string error_message = 9;\n");
        builder.append("}\n\n");
        builder.append("message GetExecutionResultRequest {\n");
        builder.append("  string tenant_id = 1;\n");
        builder.append("  string execution_id = 2;\n");
        builder.append("}\n\n");
        builder.append("message GetExecutionResultResponse {\n");
        builder.append("  repeated ").append(last.outputTypeName()).append(" items = 1;\n");
        builder.append("}\n\n");
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
        builder.append("  rpc Ingest (stream ")
            .append(first.inputTypeName())
            .append(") returns (stream ")
            .append(last.outputTypeName())
            .append(");\n");
        builder.append("  rpc RunAsync (RunAsyncRequest) returns (RunAsyncResponse);\n");
        builder.append("  rpc GetExecutionStatus (GetExecutionStatusRequest) returns (GetExecutionStatusResponse);\n");
        builder.append("  rpc GetExecutionResult (GetExecutionResultRequest) returns (GetExecutionResultResponse);\n");
        builder.append("  rpc Subscribe (google.protobuf.Empty) returns (stream ")
            .append(last.outputTypeName())
            .append(");\n");
        builder.append("}\n");
        return builder.toString();
    }

    private StreamingShape computePipelineStreamingShape(List<ResolvedStep> steps) {
        boolean inputStreaming = false;
        if (steps != null && !steps.isEmpty()) {
            inputStreaming = CardinalitySemantics.isStreamingInput(steps.get(0).cardinality());
        }
        boolean outputStreaming = inputStreaming;
        if (steps != null) {
            for (ResolvedStep step : steps) {
                outputStreaming = CardinalitySemantics.applyToOutputStreaming(step.cardinality(), outputStreaming);
            }
        }
        return new StreamingShape(inputStreaming, outputStreaming);
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
