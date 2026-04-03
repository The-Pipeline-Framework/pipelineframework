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
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
import org.pipelineframework.config.template.PipelineTemplateReserved;
import org.pipelineframework.config.template.PipelineTemplateStep;

/**
 * Generates protobuf definitions from the pipeline template configuration.
 */
public class PipelineProtoGenerator {

    private static final String ORCHESTRATOR_PROTO = "orchestrator.proto";
    private static final String TYPES_PROTO = "pipeline-types.proto";
    private static final String IDL_SNAPSHOT_PROPERTY = "tpf.idl.compat.baseline";
    private static final String IDL_SNAPSHOT_ENV = "TPF_IDL_COMPAT_BASELINE";
    private static final ObjectMapper IDL_MAPPER = PipelineJson.mapper().copy().findAndRegisterModules();

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
        generator.generate(arguments.moduleDir(), arguments.configPath(), arguments.outputDir(), arguments.typesProtoName());
    }

    /**
     * Generates protobuf definitions from the pipeline template configuration.
     *
     * This method locates and loads the pipeline configuration, creates the target
     * output directory if necessary, writes an IDL snapshot, and emits per-step
     * proto files plus the orchestrator and (when version &gt;= 2) a shared types
     * proto into the output directory.
     *
     * @param moduleDir  the module directory used to resolve the configuration and default output; may be null to use the current working directory
     * @param configPath an explicit path to the pipeline template config, or null to locate the config automatically starting from moduleDir
     * @param outputDir  the directory to write generated .proto files to, or null to use the default target/generated-sources/proto under moduleDir
     * @throws IllegalStateException if the configuration is invalid or missing required values (for example missing basePackage), if the config cannot be located, if output directories cannot be created, or if IDL compatibility checks fail
     */
    public void generate(Path moduleDir, Path configPath, Path outputDir) {
        generate(moduleDir, configPath, outputDir, TYPES_PROTO);
    }

    public void generate(Path moduleDir, Path configPath, Path outputDir, String typesProtoName) {
        Path resolvedModuleDir = moduleDir == null ? Path.of("") : moduleDir;
        Path resolvedConfig = resolveConfigPath(resolvedModuleDir, configPath);
        Path resolvedOutput = outputDir != null
            ? outputDir
            : resolvedModuleDir.resolve("target").resolve("generated-sources").resolve("proto");
        String resolvedTypesProtoName = (typesProtoName == null || typesProtoName.isBlank()) ? TYPES_PROTO : typesProtoName;

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

        List<PipelineTemplateStep> steps = config.steps();
        if (steps == null || steps.isEmpty()) {
            return;
        }
        writeIdlSnapshot(resolvedModuleDir, config);

        boolean v2 = config.version() >= 2;
        List<ResolvedStep> resolvedSteps = normalizeSteps(steps);
        List<AspectDefinition> aspectDefinitions = toAspectDefinitions(config.aspects());

        if (v2) {
            Path typesProtoPath = resolvedOutput.resolve(resolvedTypesProtoName);
            writeProto(typesProtoPath, renderTypesProto(config.basePackage(), config.messages()));
        }

        for (int i = 0; i < resolvedSteps.size(); i++) {
            ResolvedStep step = resolvedSteps.get(i);
            ResolvedStep previous = i > 0 ? resolvedSteps.get(i - 1) : null;
            String content = renderStepProto(
                config.basePackage(),
                step,
                previous,
                i == 0,
                aspectDefinitions,
                v2,
                resolvedTypesProtoName);
            Path protoPath = resolvedOutput.resolve(step.serviceName() + ".proto");
            writeProto(protoPath, content);
        }

        String transport = config.transport();
        if (transport == null || transport.isBlank() || "GRPC".equalsIgnoreCase(transport)) {
            String content = renderOrchestratorProto(config.basePackage(), resolvedSteps, v2, resolvedTypesProtoName);
            Path protoPath = resolvedOutput.resolve(ORCHESTRATOR_PROTO);
            writeProto(protoPath, content);
        }
    }

    /**
     * Write an IDL snapshot derived from the provided pipeline config into
     * moduleDir/target/generated-resources/META-INF/pipeline/idl.json and,
     * if a baseline is configured, validate the new snapshot against that baseline.
     *
     * @param moduleDir base project directory used to locate the generated-resources target path
     * @param config pipeline template configuration used to produce the IDL snapshot
     * @throws IllegalStateException if writing the snapshot fails or if IDL compatibility validation detects breaking changes
     */
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
                PipelineIdlSnapshot baselineSnapshot = readBaselineSnapshot(baseline);
                List<String> errors = new PipelineIdlCompatibilityChecker().compare(baselineSnapshot, snapshot);
                if (!errors.isEmpty()) {
                    throw new IllegalStateException("IDL compatibility check failed:\n - " + String.join("\n - ", errors));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write IDL snapshot", e);
        }
    }

    private PipelineIdlSnapshot readBaselineSnapshot(String baseline) {
        try {
            return IDL_MAPPER.readValue(Path.of(baseline).toFile(), PipelineIdlSnapshot.class);
        } catch (InvalidPathException | IOException | SecurityException e) {
            throw new IllegalStateException("Invalid IDL compatibility baseline path '" + baseline + "'", e);
        }
    }

    /**
     * Locate the IDL compatibility baseline from system property or environment variable.
     *
     * @return the baseline string trimmed, or {@code null} if not set or blank
     */
    private String resolveCompatibilityBaseline() {
        String value = System.getProperty(IDL_SNAPSHOT_PROPERTY);
        if (value == null || value.isBlank()) {
            value = System.getenv(IDL_SNAPSHOT_ENV);
        }
        return value == null || value.isBlank() ? null : value.trim();
    }

    /**
     * Resolve the pipeline template configuration path by returning the provided path or locating a config under the module directory.
     *
     * @param moduleDir the root directory to search for a pipeline template config when {@code configPath} is null
     * @param configPath an explicit path to the pipeline template config; if non-null this value is returned
     * @return the resolved path to the pipeline template configuration
     * @throws IllegalStateException if {@code configPath} is null and no pipeline template config can be located under {@code moduleDir}
     */
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

    /**
     * Return an immutable shallow copy of the provided field list, or an empty list when no fields are provided.
     *
     * @param fields the list of fields to copy; may be null
     * @return an unmodifiable list containing the same `PipelineTemplateField` elements as the input, or an empty list if `fields` is null or empty
     */
    private List<PipelineTemplateField> copyFields(List<PipelineTemplateField> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        return List.copyOf(fields);
    }

    /**
     * Produce the text content of a protobuf file that declares all provided message types
     * under the specified base package.
     *
     * @param basePackage the protobuf package and Java package base to use for the generated file
     * @param messages a map of message identifiers to PipelineTemplateMessage instances; each value
     *                 is rendered as a protobuf `message` (map keys are not used for rendering)
     * @return the complete .proto file content containing syntax, package, java_package options and
     *         the rendered message definitions
     */
    private String renderTypesProto(String basePackage, Map<String, PipelineTemplateMessage> messages) {
        Map<String, PipelineTemplateMessage> safeMessages = messages == null ? Map.of() : new LinkedHashMap<>(messages);
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");
        builder.append("option java_outer_classname = \"PipelineTypes\";\n\n");
        boolean first = true;
        List<String> messageNames = new ArrayList<>(safeMessages.keySet());
        Collections.sort(messageNames);
        for (String messageName : messageNames) {
            PipelineTemplateMessage message = safeMessages.get(messageName);
            if (!first) {
                builder.append('\n');
            }
            renderMessage(builder, message);
            first = false;
        }
        return builder.toString();
    }

    /**
     * Builds the .proto file content for a single pipeline step.
     *
     * @param basePackage the protobuf package and Java package base to use in the generated proto
     * @param step the resolved step to render (contains names, types, fields, and cardinality)
     * @param previous the resolved previous step, or {@code null} if there is none
     * @param firstStep {@code true} when rendering the pipeline's first step (affects legacy input rendering)
     * @param aspects list of aspect definitions whose services should be rendered alongside the step
     * @param v2 {@code true} to render the step using the v2/types-based proto layout; {@code false} to use legacy imports/messages
     * @return the complete proto file content for the given step as a String
     */
    private String renderStepProto(
        String basePackage,
        ResolvedStep step,
        ResolvedStep previous,
        boolean firstStep,
        List<AspectDefinition> aspects,
        boolean v2,
        String typesProtoName
    ) {
        StringBuilder builder = new StringBuilder();
        builder.append("syntax = \"proto3\";\n\n");
        builder.append("package ").append(basePackage).append(";\n\n");
        builder.append("option java_package = \"")
            .append(basePackage)
            .append(".grpc\";\n\n");

        if (v2) {
            builder.append("import \"").append(typesProtoName).append("\";\n\n");
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

    /**
     * Appends a protobuf `message` definition for the given PipelineTemplateMessage to the provided StringBuilder.
     *
     * The generated block includes the message declaration, any reserved numeric ranges and reserved names,
     * all field lines produced by renderFieldLine(...), and a closing brace.
     *
     * @param builder destination StringBuilder to which the message definition is appended
     * @param message source PipelineTemplateMessage containing the message name, reserved entries, and fields
     */
    private void renderMessage(StringBuilder builder, PipelineTemplateMessage message) {
        builder.append("message ").append(message.name()).append(" {\n");
        PipelineTemplateReserved reserved = message.reserved();
        List<PipelineTemplateField> fields = message.fields() == null ? List.of() : message.fields();
        if (reserved != null && reserved.numbers() != null && !reserved.numbers().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < reserved.numbers().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(reserved.numbers().get(i));
            }
            builder.append(";\n");
        }
        if (reserved != null && reserved.names() != null && !reserved.names().isEmpty()) {
            builder.append("  reserved ");
            for (int i = 0; i < reserved.names().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append('"').append(reserved.names().get(i)).append('"');
            }
            builder.append(";\n");
        }
        for (PipelineTemplateField field : fields) {
            if (field != null) {
                renderFieldLine(builder, field);
            }
        }
        builder.append("}\n");
    }

    /**
     * Appends a legacy-style protobuf message definition for the given type to the provided builder.
     *
     * @param builder the StringBuilder to append the message definition to
     * @param typeName the protobuf message name
     * @param fields the list of fields to render; null, null entries, or fields with null/blank names are skipped
     * @param startNumber the field number to assign to the first rendered field; subsequent fields are numbered sequentially
     */
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

    /**
     * Appends a single protobuf field declaration (and an optional preceding comment) to the provided builder.
     *
     * The rendered line includes the field type, name, number, and an optional deprecated option.
     * If the field is marked repeated, the `repeated` label is emitted; otherwise, if the field is marked optional
     * and optional labels are supported for that field, the `optional` label is emitted.
     *
     * @param builder the StringBuilder to append the field comment and declaration to
     * @param field the field definition containing comment, labels (repeated/optional), type, name, number, and deprecation flag
     */
    private void renderFieldLine(StringBuilder builder, PipelineTemplateField field) {
        if (field.comment() != null && !field.comment().isBlank()) {
            for (String rawLine : field.comment().split("\\R")) {
                String line = rawLine == null ? null : rawLine.trim();
                if (line != null && !line.isEmpty()) {
                    builder.append("  // ").append(line).append('\n');
                }
            }
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

    /**
     * Determine whether the protobuf `optional` label is allowed for the given field.
     *
     * @param field the field to evaluate
     * @return `true` if the field supports the `optional` label (it is neither a map nor a message reference), `false` otherwise
     */
    private boolean supportsOptionalLabel(PipelineTemplateField field) {
        return !field.isMap() && !field.isMessageReference();
    }

    /**
     * Compute the legacy protobuf field type declaration for a PipelineTemplateField.
     *
     * @param field the template field to render the legacy protobuf type for
     * @return the protobuf type string, prefixed with "repeated " if the field is repeated; `"string"` if the field's protoType is null or blank, otherwise the field's protoType
     */
    private String renderLegacyFieldType(PipelineTemplateField field) {
        String baseType = field.protoType();
        if (baseType == null || baseType.isBlank()) {
            System.getLogger(PipelineProtoGenerator.class.getName()).log(
                System.Logger.Level.WARNING,
                "Legacy field '{0}' is missing protoType; defaulting to string",
                field.name());
            baseType = "string";
        }
        if (field.repeated()) {
            return "repeated " + baseType;
        }
        return baseType;
    }

    /**
     * Appends a protobuf service definition for the given pipeline step to the provided builder.
     *
     * The generated service is named "Process{ServiceNameFormatted}Service" and defines a single
     * RPC `remoteProcess` whose streaming modifiers are chosen based on the step's cardinality:
     * ONE_TO_MANY => unary input, streaming output;
     * MANY_TO_MANY => streaming input, streaming output;
     * MANY_TO_ONE => streaming input, unary output;
     * otherwise => unary input, unary output.
     *
     * @param step the resolved step to render the service for
     * @param previous the resolved previous step, or {@code null} if none; used to determine the input type when this is not the first step
     * @param firstStep whether {@code step} is the pipeline's first step (affects input type selection)
     */
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

    /**
     * Appends a protobuf service definition for an aspect that observes a given message type.
     *
     * @param builder  the StringBuilder to append the service definition to
     * @param aspect   the aspect definition whose name is used to derive the service name
     * @param typeName the protobuf message type name used as the rpc input and output
     */
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

    /**
     * Generate the orchestrator .proto file content for a pipeline.
     *
     * When `v2` is true the generated proto imports the centralized types proto; otherwise it imports
     * the first and (if different) last step protos. The proto includes request/response messages for
     * run, async run, execution status/result and a OrchestratorService with RPCs whose streaming
     * modifiers reflect the pipeline's input/output streaming shape.
     *
     * @param basePackage the protobuf package and Java package base for generated types
     * @param steps the pipeline steps already normalized into ResolvedStep objects
     * @param v2 if true, import and reference the shared types proto instead of per-step protos
     * @return the complete orchestrator .proto file content as a String
     */
    private String renderOrchestratorProto(String basePackage, List<ResolvedStep> steps, boolean v2, String typesProtoName) {
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
            builder.append("import \"").append(typesProtoName).append("\";\n");
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

    /**
     * Removes a leading "Process " prefix from the provided name.
     *
     * @param name the input name to normalize; may be null
     * @return the input name with the leading "Process " prefix removed (case-sensitive),
     *         the original name if the prefix is not present, or an empty string if {@code name} is null
     */
    private String stripProcessPrefix(String name) {
        if (name == null) {
            return "";
        }
        if (name.startsWith("Process ")) {
            return name.substring("Process ".length());
        }
        return name;
    }

    /**
     * Builds the observe service name for an aspect and message type.
     *
     * @param aspectName the aspect's name (may contain non-alphanumeric separators); trimmed and converted to PascalCase
     * @param typeName   the message/type name to append; trimmed before use
     * @return the constructed service name in the form `Observe{AspectPascal}{TypeName}SideEffectService`, or an empty string if either argument is null or blank
     */
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

    private record Arguments(Path moduleDir, Path configPath, Path outputDir, String typesProtoName) {
        /**
         * Parse command-line options and produce an Arguments instance with the resolved paths.
         *
         * Recognized options:
         * - --module-dir <path> or --module-dir=<path>
         * - --config <path> or --config=<path>
         * - --output-dir <path> or --output-dir=<path>
         * - --types-proto-name <file> or --types-proto-name=<file>
         * - --help or -h (prints usage and exits)
         *
         * @param args the raw CLI arguments (may be null)
         * @return an Arguments object with moduleDir, configPath, and outputDir set from the parsed options (any unset value will be null)
         */
        static Arguments parse(String[] args) {
            Path moduleDir = null;
            Path configPath = null;
            Path outputDir = null;
            String typesProtoName = TYPES_PROTO;
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
                    } else if (arg.startsWith("--types-proto-name=")) {
                        typesProtoName = arg.substring("--types-proto-name=".length());
                    } else if ("--types-proto-name".equals(arg) && i + 1 < args.length) {
                        typesProtoName = args[++i];
                    } else if ("--help".equals(arg) || "-h".equals(arg)) {
                        printUsage();
                        System.exit(0);
                    }
                }
            }
            return new Arguments(moduleDir, configPath, outputDir, typesProtoName);
        }

        /**
         * Display the command-line usage instructions for PipelineProtoGenerator.
         *
         * Writes a single-line usage message to standard output describing the accepted options: --module-dir, --config, and --output-dir.
         */
        static void printUsage() {
            System.out.println(
                "Usage: PipelineProtoGenerator [--module-dir DIR] [--config PATH] [--output-dir DIR] [--types-proto-name FILE]");
        }
    }
}
