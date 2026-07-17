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

import java.util.*;

/**
 * Normalized IDL snapshot used for compatibility checking and build metadata emission.
 *
 * @param version template config version
 * @param appName application name
 * @param basePackage base package
 * @param messages normalized messages
 * @param unions normalized closed unions
 * @param steps normalized step input/output references
 */
public record PipelineIdlSnapshot(
    int version,
    String appName,
    String basePackage,
    Map<String, MessageSnapshot> messages,
    Map<String, UnionSnapshot> unions,
    Map<String, TypeSnapshot> types,
    List<StepSnapshot> steps
) {
    public PipelineIdlSnapshot {
        messages = messages == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(messages));
        unions = unions == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(unions));
        types = types == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(types));
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public PipelineIdlSnapshot(
        int version,
        String appName,
        String basePackage,
        Map<String, MessageSnapshot> messages,
        List<StepSnapshot> steps
    ) {
        this(version, appName, basePackage, messages, Map.of(), Map.of(), steps);
    }

    public PipelineIdlSnapshot(
        int version,
        String appName,
        String basePackage,
        Map<String, MessageSnapshot> messages,
        Map<String, UnionSnapshot> unions,
        List<StepSnapshot> steps
    ) {
        this(version, appName, basePackage, messages, unions, Map.of(), steps);
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
        Map<String, UnionSnapshot> unions = new LinkedHashMap<>();
        if (config.unions() != null && !config.unions().isEmpty()) {
            for (Map.Entry<String, PipelineTemplateUnion> entry : config.unions().entrySet()) {
                unions.put(entry.getKey(), toUnionSnapshot(entry.getValue()));
            }
        }
        List<StepSnapshot> steps = new ArrayList<>();
        for (PipelineTemplateStep step : configSteps) {
            steps.add(new StepSnapshot(step.name(), step.inputTypeName(), step.outputTypeName()));
        }
        Map<String, TypeSnapshot> types = config.dialect() == PipelineTemplateDialect.V3
            ? toTypeSnapshots(config.typeModel()) : Map.of();
        return new PipelineIdlSnapshot(config.version(), config.appName(), config.basePackage(), messages, unions, types, steps);
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
        Set<Integer> usedNumbers = new HashSet<>();
        for (PipelineTemplateField field : fields) {
            if (field.number() != null && !usedNumbers.add(field.number())) {
                throw new IllegalStateException(
                    "Conflicting legacy field number " + field.number() + " in message '" + messageName + "'");
            }
        }
        int nextAutoNumber = 1;
        for (int i = 0; i < fields.size(); i++) {
            PipelineTemplateField field = fields.get(i);
            int resolvedNumber = field.number() == null ? nextAvailableNumber(usedNumbers, nextAutoNumber) : field.number();
            usedNumbers.add(resolvedNumber);
            nextAutoNumber = resolvedNumber + 1;
            snapshotFields.add(new FieldSnapshot(
                resolvedNumber,
                field.name(),
                field.canonicalType(),
                field.messageRef(),
                field.keyType(),
                field.valueType(),
                field.optional(),
                field.repeated(),
                field.deprecated(),
                field.protoType(),
                field.referenceable() == null ? null : field.referenceable().refField()));
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

    private static int nextAvailableNumber(Set<Integer> usedNumbers, int startingAt) {
        int number = startingAt;
        while (usedNumbers.contains(number)) {
            number++;
        }
        return number;
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
                field.protoType(),
                field.referenceable() == null ? null : field.referenceable().refField()));
        }
        return new MessageSnapshot(
            message.name(),
            fields,
            reserved.numbers(),
            reserved.names());
    }

    private static UnionSnapshot toUnionSnapshot(PipelineTemplateUnion union) {
        List<UnionVariantSnapshot> variants = union.variants().values().stream()
            .sorted(java.util.Comparator.comparingInt(PipelineTemplateUnionVariant::number))
            .map(variant -> new UnionVariantSnapshot(variant.name(), variant.type(), variant.number()))
            .toList();
        return new UnionSnapshot(union.name(), variants);
    }

    private static Map<String, TypeSnapshot> toTypeSnapshots(PipelineTemplateTypeModel typeModel) {
        Map<String, TypeSnapshot> result = new LinkedHashMap<>();
        PipelineIdlTagAllocator allocator = new PipelineIdlTagAllocator();
        typeModel.definitions().forEach((name, definition) -> {
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                List<TypeFieldSnapshot> fields = new ArrayList<>();
                Set<Integer> unavailable = new HashSet<>();
                for (PipelineTemplateTypeDefinition.Field field : record.fields().stream()
                    .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Field::name)).toList()) {
                    int number = allocator.allocate(unavailable);
                    unavailable.add(number);
                    fields.add(new TypeFieldSnapshot(number, field.name(),
                        PipelineIdlStateResolver.toProtoFieldName(field.name()), field.type().name()));
                }
                result.put(name, new TypeSnapshot(name, "record", fields, Optional.empty(), List.of()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                result.put(name, new TypeSnapshot(name, "wrapper", List.of(), Optional.of(wrapper.wraps().name()), List.of()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
                result.put(name, new TypeSnapshot(name, "alias", List.of(), Optional.of(alias.target().name()), List.of()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                List<TypeVariantSnapshot> variants = new ArrayList<>();
                Set<Integer> unavailable = new HashSet<>();
                for (PipelineTemplateTypeDefinition.Variant variant : union.variants().values().stream()
                    .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Variant::discriminator)).toList()) {
                    int number = allocator.allocate(unavailable);
                    unavailable.add(number);
                    variants.add(new TypeVariantSnapshot(variant.discriminator(), variant.payload().name(),
                        PipelineIdlStateResolver.toProtoFieldName(variant.discriminator()), number));
                }
                result.put(name, new TypeSnapshot(name, "union", List.of(), Optional.empty(), variants));
            }
        });
        return result;
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
        String protoType,
        String referenceField
    ) {
    }

    public record UnionSnapshot(
        String name,
        List<UnionVariantSnapshot> variants
    ) {
        public UnionSnapshot {
            variants = variants == null ? List.of() : List.copyOf(variants);
        }
    }

    public record UnionVariantSnapshot(
        String name,
        String type,
        int number
    ) {
    }

    /** Compiler-owned state for a v3 semantic type. */
    public record TypeSnapshot(
        String name,
        String kind,
        List<TypeFieldSnapshot> fields,
        Optional<String> target,
        List<TypeVariantSnapshot> variants,
        List<Integer> reservedNumbers,
        List<String> reservedNames
    ) {
        public TypeSnapshot {
            fields = fields == null ? List.of() : List.copyOf(fields);
            target = target == null ? Optional.empty() : target;
            variants = variants == null ? List.of() : List.copyOf(variants);
            reservedNumbers = reservedNumbers == null ? List.of() : List.copyOf(reservedNumbers);
            reservedNames = reservedNames == null ? List.of() : List.copyOf(reservedNames);
        }

        public TypeSnapshot(String name, String kind, List<TypeFieldSnapshot> fields, Optional<String> target,
                            List<TypeVariantSnapshot> variants) {
            this(name, kind, fields, target, variants, List.of(), List.of());
        }
    }

    public record TypeFieldSnapshot(int number, String name, String protoName, String type) {
        public TypeFieldSnapshot(int number, String name, String type) {
            this(number, name, name, type);
        }
    }

    public record TypeVariantSnapshot(String discriminator, String payload, String protoName, int number) {
    }

    public record StepSnapshot(
        String name,
        String inputTypeName,
        String outputTypeName
    ) {
    }
}
