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
        messages = messages == null ? Map.of() : Map.copyOf(messages);
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public static PipelineIdlSnapshot from(PipelineTemplateConfig config) {
        Map<String, MessageSnapshot> messages = new LinkedHashMap<>();
        if (config.messages() != null && !config.messages().isEmpty()) {
            for (Map.Entry<String, PipelineTemplateMessage> entry : config.messages().entrySet()) {
                messages.put(entry.getKey(), toMessageSnapshot(entry.getValue()));
            }
        } else {
            collectLegacyMessages(messages, config.steps());
        }
        List<StepSnapshot> steps = new ArrayList<>();
        for (PipelineTemplateStep step : config.steps()) {
            steps.add(new StepSnapshot(step.name(), step.inputTypeName(), step.outputTypeName()));
        }
        return new PipelineIdlSnapshot(config.version(), config.appName(), config.basePackage(), messages, steps);
    }

    private static void collectLegacyMessages(
        Map<String, MessageSnapshot> messages,
        List<PipelineTemplateStep> steps
    ) {
        for (PipelineTemplateStep step : steps) {
            putLegacyMessage(messages, step.inputTypeName(), step.inputFields());
            putLegacyMessage(messages, step.outputTypeName(), step.outputFields());
        }
    }

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
        messages.putIfAbsent(messageName, new MessageSnapshot(messageName, snapshotFields, List.of(), List.of()));
    }

    private static MessageSnapshot toMessageSnapshot(PipelineTemplateMessage message) {
        List<FieldSnapshot> fields = new ArrayList<>();
        for (PipelineTemplateField field : message.fields()) {
            fields.add(new FieldSnapshot(
                field.number() == null ? 0 : field.number(),
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
            message.reserved().numbers(),
            message.reserved().names());
    }

    public record MessageSnapshot(
        String name,
        List<FieldSnapshot> fields,
        List<Integer> reservedNumbers,
        List<String> reservedNames
    ) {
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
