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

package org.pipelineframework.config.template;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Normalized IDL snapshot used for compatibility checking and build metadata emission.
 *
 * @param version template config version
 * @param appName application name
 * @param basePackage base package
 * @param messages normalized messages
 * @param steps normalized step input/output references
 */
public record PipelineIdlSnapshot(
    int version,
    String appName,
    String basePackage,
    Map<String, MessageSnapshot> messages,
    List<StepSnapshot> steps
) {
    public PipelineIdlSnapshot {
        messages = messages == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(messages));
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    /**
     * Create a normalized PipelineIdlSnapshot from a PipelineTemplateConfig.
     *
     * <p>The returned snapshot contains the config's version, app name, base package, a map of
     * MessageSnapshot objects (constructed from config.messages() when present or derived from legacy
     * step definitions), and a list of StepSnapshot objects built from config.steps().
     *
     * @param config the pipeline template configuration to convert into a snapshot
     * @return a PipelineIdlSnapshot containing normalized messages and steps extracted from the config
     */
    public static PipelineIdlSnapshot from(PipelineTemplateConfig config) {
        List<PipelineTemplateStep> configSteps = config.steps() == null ? List.of() : config.steps();
        Map<String, MessageSnapshot> messages = new LinkedHashMap<>();
        if (config.messages() != null && !config.messages().isEmpty()) {
            for (Map.Entry<String, PipelineTemplateMessage> entry : config.messages().entrySet()) {
                messages.put(entry.getKey(), toMessageSnapshot(entry.getValue()));
            }
        } else {
            collectLegacyMessages(messages, configSteps);
        }
        List<StepSnapshot> steps = new ArrayList<>();
        for (PipelineTemplateStep step : configSteps) {
            steps.add(new StepSnapshot(step.name(), step.inputTypeName(), step.outputTypeName()));
        }
        return new PipelineIdlSnapshot(config.version(), config.appName(), config.basePackage(), messages, steps);
    }

    /**
     * Populate the provided messages map with MessageSnapshot entries derived from legacy step definitions.
     *
     * For each step, adds message snapshots for the step's input and output type names when those types
     * have field definitions; existing entries are preserved.
     *
     * @param messages the map to populate or update with message name -> MessageSnapshot entries
     * @param steps the list of pipeline template steps to inspect for input/output message definitions
     */
    private static void collectLegacyMessages(
        Map<String, MessageSnapshot> messages,
        List<PipelineTemplateStep> steps
    ) {
        for (PipelineTemplateStep step : steps) {
            putLegacyMessage(messages, step.inputTypeName(), step.inputFields());
            putLegacyMessage(messages, step.outputTypeName(), step.outputFields());
        }
    }

    /**
     * Adds a legacy message snapshot to the provided messages map when a valid name and fields are present.
     *
     * If a snapshot for the given message name already exists, the method verifies that the shape matches.
     *
     * @param messages    the map to populate with the constructed MessageSnapshot
     * @param messageName the name of the message to add; ignored if null or blank
     * @param fields      the legacy fields used to build the message; ignored if null or empty
     */
    private static void putLegacyMessage(
        Map<String, MessageSnapshot> messages,
        String messageName,
        List<PipelineTemplateField> fields
    ) {
        if (messageName == null || messageName.isBlank() || fields == null || fields.isEmpty()) {
            return;
        }
        List<FieldSnapshot> snapshotFields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            PipelineTemplateField field = fields.get(i);
            snapshotFields.add(new FieldSnapshot(
                field.number() == null ? i + 1 : field.number(),
                field.name(),
                field.canonicalType(),
                field.messageRef(),
                field.keyType(),
                field.valueType(),
                field.optional(),
                field.repeated(),
                field.deprecated(),
                field.protoType()));
        }
        MessageSnapshot candidate = new MessageSnapshot(messageName, snapshotFields, List.of(), List.of());
        MessageSnapshot existing = messages.get(messageName);
        if (existing != null) {
            if (!existing.equals(candidate)) {
                throw new IllegalStateException("Conflicting legacy message shape for '" + messageName + "'");
            }
            return;
        }
        messages.put(messageName, candidate);
    }

    /**
     * Convert a PipelineTemplateMessage into a MessageSnapshot.
     *
     * @param message the template message to convert
     * @return a MessageSnapshot containing the message name, its fields, reserved numbers, and reserved names
     * @throws IllegalStateException if any field in the template message does not have a number
     */
    private static MessageSnapshot toMessageSnapshot(PipelineTemplateMessage message) {
        List<PipelineTemplateField> messageFields = message.fields() == null ? List.of() : message.fields();
        PipelineTemplateReserved reserved = message.reserved() == null
            ? new PipelineTemplateReserved(List.of(), List.of())
            : message.reserved();
        List<FieldSnapshot> fields = new ArrayList<>();
        for (PipelineTemplateField field : messageFields) {
            if (field.number() == null) {
                throw new IllegalStateException(
                    "Message '" + message.name() + "' field '" + field.name() + "' is missing required field number");
            }
            fields.add(new FieldSnapshot(
                field.number(),
                field.name(),
                field.canonicalType(),
                field.messageRef(),
                field.keyType(),
                field.valueType(),
                field.optional(),
                field.repeated(),
                field.deprecated(),
                field.protoType()));
        }
        return new MessageSnapshot(
            message.name(),
            fields,
            reserved.numbers(),
            reserved.names());
    }

    public record MessageSnapshot(
        String name,
        List<FieldSnapshot> fields,
        List<Integer> reservedNumbers,
        List<String> reservedNames
    ) {
        public MessageSnapshot {
            fields = fields == null ? List.of() : List.copyOf(fields);
            reservedNumbers = reservedNumbers == null ? List.of() : List.copyOf(reservedNumbers);
            reservedNames = reservedNames == null ? List.of() : List.copyOf(reservedNames);
        }
    }

    public record FieldSnapshot(
        int number,
        String name,
        String canonicalType,
        String messageRef,
        String keyType,
        String valueType,
        boolean optional,
        boolean repeated,
        boolean deprecated,
        String protoType
    ) {
    }

    public record StepSnapshot(
        String name,
        String inputTypeName,
        String outputTypeName
    ) {
    }
}
