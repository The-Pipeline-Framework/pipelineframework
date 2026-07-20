/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.config.template;

import java.util.*;

/** Resolves compiler-owned protobuf tags from persisted IDL state. */
public final class PipelineIdlStateResolver {

    private final PipelineIdlTagAllocator allocator = new PipelineIdlTagAllocator();

    public Resolved resolve(PipelineTemplateConfig config, PipelineIdlSnapshot baseline, boolean bootstrap) {
        if (config.dialect() == PipelineTemplateDialect.V3) {
            if (baseline == null && !bootstrap) {
                throw new IllegalStateException(
                    "Tag-free template fields require committed pipeline.idl.json; run the explicit bootstrap first");
            }
            return new Resolved(config, resolveV3State(config, baseline));
        }
        if (config.version() < 2 || config.messages().isEmpty()) {
            return new Resolved(config, PipelineIdlSnapshot.from(config));
        }
        boolean concise = config.messages().values().stream()
            .flatMap(message -> message.fields().stream()).anyMatch(field -> field.number() == null)
            || config.unions().values().stream().flatMap(union -> union.variants().values().stream())
                .anyMatch(variant -> variant.number() == null);
        if (baseline == null && concise && !bootstrap) {
            throw new IllegalStateException(
                "Tag-free template fields require committed pipeline.idl.json; run the explicit bootstrap first");
        }
        Map<String, PipelineIdlSnapshot.MessageSnapshot> previousMessages = baseline == null
            ? Map.of() : baseline.messages();
        Map<String, PipelineTemplateMessage> messages = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateMessage> entry : config.messages().entrySet()) {
            PipelineIdlSnapshot.MessageSnapshot previous = previousMessages.get(entry.getKey());
            messages.put(entry.getKey(), resolveMessage(entry.getValue(), previous));
        }
        Map<String, PipelineIdlSnapshot.UnionSnapshot> previousUnions = baseline == null ? Map.of() : baseline.unions();
        Map<String, PipelineTemplateUnion> unions = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateUnion> entry : config.unions().entrySet()) {
            unions.put(entry.getKey(), resolveUnion(entry.getValue(), previousUnions.get(entry.getKey())));
        }
        PipelineTemplateConfig resolved = new PipelineTemplateConfig(config.version(), config.appName(), config.basePackage(),
            config.transport(), config.platform(), messages, unions, config.sources(), config.publish(), config.steps(),
            config.aspects(), config.input(), config.output(), config.materialization(), config.inputContract(), config.outputContract());
        return new Resolved(resolved, PipelineIdlSnapshot.from(resolved));
    }

    private PipelineIdlSnapshot resolveV3State(PipelineTemplateConfig config, PipelineIdlSnapshot baseline) {
        Map<String, PipelineIdlSnapshot.TypeSnapshot> types = new LinkedHashMap<>();
        for (Map.Entry<String, PipelineTemplateTypeDefinition> entry : config.typeModel().definitions().entrySet()) {
            String name = entry.getKey();
            PipelineTemplateTypeDefinition definition = entry.getValue();
            PipelineIdlSnapshot.TypeSnapshot previous = baseline == null ? null : baseline.types().get(name);
            if (definition instanceof PipelineTemplateTypeDefinition.RecordType record) {
                Map<String, Integer> priorNumbers = previous == null ? Map.of() : previous.fields().stream()
                    .collect(java.util.stream.Collectors.toMap(PipelineIdlSnapshot.TypeFieldSnapshot::name,
                        PipelineIdlSnapshot.TypeFieldSnapshot::number));
                Map<String, String> priorProtoNames = previous == null ? Map.of() : previous.fields().stream()
                    .collect(java.util.stream.Collectors.toMap(PipelineIdlSnapshot.TypeFieldSnapshot::name,
                        PipelineIdlSnapshot.TypeFieldSnapshot::protoName));
                Set<Integer> unavailable = previous == null ? new HashSet<>() : previous.fields().stream()
                    .map(PipelineIdlSnapshot.TypeFieldSnapshot::number).collect(java.util.stream.Collectors.toSet());
                List<Integer> reservedNumbers = previous == null ? new ArrayList<>() : new ArrayList<>(previous.reservedNumbers());
                List<String> reservedNames = previous == null ? new ArrayList<>() : new ArrayList<>(previous.reservedNames());
                Set<String> currentNames = record.fields().stream().map(PipelineTemplateTypeDefinition.Field::name)
                    .collect(java.util.stream.Collectors.toSet());
                if (previous == null && baseline != null && baseline.messages().containsKey(name)) {
                    PipelineIdlSnapshot.MessageSnapshot legacy = baseline.messages().get(name);
                    priorNumbers = legacy.fields().stream().collect(java.util.stream.Collectors.toMap(
                        PipelineIdlSnapshot.FieldSnapshot::name, PipelineIdlSnapshot.FieldSnapshot::number));
                    unavailable = new HashSet<>(priorNumbers.values());
                    reservedNumbers.addAll(legacy.reservedNumbers());
                    reservedNames.addAll(legacy.reservedNames());
                    for (PipelineIdlSnapshot.FieldSnapshot field : legacy.fields()) {
                        if (!currentNames.contains(field.name())) {
                            reservedNumbers.add(field.number());
                            reservedNames.add(toProtoFieldName(field.name()));
                            unavailable.add(field.number());
                        }
                    }
                }
                unavailable.addAll(reservedNumbers);
                if (previous != null) {
                    for (PipelineIdlSnapshot.TypeFieldSnapshot field : previous.fields()) {
                        if (!currentNames.contains(field.name())) {
                            reservedNumbers.add(field.number());
                            reservedNames.add(field.protoName());
                            unavailable.add(field.number());
                        }
                    }
                }
                List<PipelineIdlSnapshot.TypeFieldSnapshot> fields = new ArrayList<>();
                Set<String> protoNames = new HashSet<>(reservedNames);
                for (PipelineTemplateTypeDefinition.Field field : record.fields().stream()
                    .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Field::name)).toList()) {
                    Integer number = priorNumbers.get(field.name());
                    if (number == null) { number = allocator.allocate(unavailable); }
                    unavailable.add(number);
                    String protoName = priorProtoNames.getOrDefault(field.name(), toProtoFieldName(field.name()));
                    if (protoName.isBlank() || !protoNames.add(protoName)) {
                        throw new IllegalStateException("Type '" + name + "' has colliding protobuf field name '" + protoName + "'.");
                    }
                    fields.add(new PipelineIdlSnapshot.TypeFieldSnapshot(number, field.name(), protoName, field.type().name()));
                }
                types.put(name, new PipelineIdlSnapshot.TypeSnapshot(name, "record", fields, Optional.empty(), List.of(),
                    reservedNumbers.stream().distinct().sorted().toList(), reservedNames.stream().distinct().sorted().toList()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.WrapperType wrapper) {
                types.put(name, new PipelineIdlSnapshot.TypeSnapshot(name, "wrapper", List.of(), Optional.of(wrapper.wraps().name()), List.of(),
                    List.of(), List.of(), wrapper.constraints()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.AliasType alias) {
                types.put(name, new PipelineIdlSnapshot.TypeSnapshot(name, "alias", List.of(), Optional.of(alias.target().name()), List.of()));
            } else if (definition instanceof PipelineTemplateTypeDefinition.UnionType union) {
                Map<String, Integer> priorNumbers = previous == null ? Map.of() : previous.variants().stream()
                    .collect(java.util.stream.Collectors.toMap(PipelineIdlSnapshot.TypeVariantSnapshot::discriminator,
                        PipelineIdlSnapshot.TypeVariantSnapshot::number));
                Map<String, String> priorProtoNames = previous == null ? Map.of() : previous.variants().stream()
                    .collect(java.util.stream.Collectors.toMap(PipelineIdlSnapshot.TypeVariantSnapshot::discriminator,
                        PipelineIdlSnapshot.TypeVariantSnapshot::protoName));
                Set<Integer> unavailable = previous == null ? new HashSet<>() : previous.variants().stream()
                    .map(PipelineIdlSnapshot.TypeVariantSnapshot::number).collect(java.util.stream.Collectors.toSet());
                List<Integer> reservedNumbers = previous == null ? new ArrayList<>() : new ArrayList<>(previous.reservedNumbers());
                List<String> reservedNames = previous == null ? new ArrayList<>() : new ArrayList<>(previous.reservedNames());
                Set<String> currentNames = union.variants().keySet();
                if (previous == null && baseline != null && baseline.unions().containsKey(name)) {
                    PipelineIdlSnapshot.UnionSnapshot legacy = baseline.unions().get(name);
                    priorNumbers = legacy.variants().stream().collect(java.util.stream.Collectors.toMap(
                        PipelineIdlSnapshot.UnionVariantSnapshot::name, PipelineIdlSnapshot.UnionVariantSnapshot::number));
                    unavailable = new HashSet<>(priorNumbers.values());
                    for (PipelineIdlSnapshot.UnionVariantSnapshot variant : legacy.variants()) {
                        if (!currentNames.contains(variant.name())) {
                            reservedNumbers.add(variant.number());
                            reservedNames.add(toProtoFieldName(variant.name()));
                            unavailable.add(variant.number());
                        }
                    }
                }
                unavailable.addAll(reservedNumbers);
                if (previous != null) {
                    for (PipelineIdlSnapshot.TypeVariantSnapshot variant : previous.variants()) {
                        if (!currentNames.contains(variant.discriminator())) {
                            reservedNumbers.add(variant.number());
                            reservedNames.add(variant.protoName());
                            unavailable.add(variant.number());
                        }
                    }
                }
                List<PipelineIdlSnapshot.TypeVariantSnapshot> variants = new ArrayList<>();
                Set<String> protoNames = new HashSet<>(reservedNames);
                for (PipelineTemplateTypeDefinition.Variant variant : union.variants().values().stream()
                    .sorted(Comparator.comparing(PipelineTemplateTypeDefinition.Variant::discriminator)).toList()) {
                    Integer number = priorNumbers.get(variant.discriminator());
                    if (number == null) { number = allocator.allocate(unavailable); }
                    unavailable.add(number);
                    String protoName = priorProtoNames.getOrDefault(variant.discriminator(),
                        toProtoFieldName(variant.discriminator()));
                    if (protoName.isBlank() || !protoNames.add(protoName)) {
                        throw new IllegalStateException("Union '" + name + "' has colliding protobuf discriminator field '" + protoName + "'.");
                    }
                    variants.add(new PipelineIdlSnapshot.TypeVariantSnapshot(variant.discriminator(), variant.payload().name(), protoName, number));
                }
                types.put(name, new PipelineIdlSnapshot.TypeSnapshot(name, "union", List.of(), Optional.empty(), variants,
                    reservedNumbers.stream().distinct().sorted().toList(), reservedNames.stream().distinct().sorted().toList()));
            }
        }
        List<PipelineIdlSnapshot.StepSnapshot> steps = config.steps().stream()
            .map(step -> new PipelineIdlSnapshot.StepSnapshot(step.name(), step.inputTypeName(), step.outputTypeName())).toList();
        return new PipelineIdlSnapshot(config.version(), config.appName(), config.basePackage(), Map.of(), Map.of(), types, steps);
    }

    static String toProtoFieldName(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        char previous = 0;
        for (int i = 0; i < input.length(); i++) {
            char current = input.charAt(i);
            if (!Character.isLetterOrDigit(current)) {
                if (!builder.isEmpty() && builder.charAt(builder.length() - 1) != '_') { builder.append('_'); }
            } else {
                if (Character.isUpperCase(current) && i > 0 && previous != 0 && previous != '_'
                    && Character.isLowerCase(previous)) { builder.append('_'); }
                builder.append(Character.toLowerCase(current));
            }
            previous = current;
        }
        return builder.toString().replaceAll("^_+|_+$", "");
    }

    private PipelineTemplateMessage resolveMessage(
        PipelineTemplateMessage message, PipelineIdlSnapshot.MessageSnapshot previous
    ) {
        Map<String, PipelineIdlSnapshot.FieldSnapshot> oldByName = new HashMap<>();
        Set<Integer> unavailable = new HashSet<>();
        List<Integer> reservedNumbers = new ArrayList<>(message.reserved().numbers());
        List<String> reservedNames = new ArrayList<>(message.reserved().names());
        if (previous != null) {
            previous.fields().forEach(field -> { oldByName.put(field.name(), field); unavailable.add(field.number()); });
            unavailable.addAll(previous.reservedNumbers());
            reservedNumbers.addAll(previous.reservedNumbers());
            reservedNames.addAll(previous.reservedNames());
        }
        unavailable.addAll(reservedNumbers);
        Set<String> currentNames = new HashSet<>();
        for (PipelineTemplateField field : message.fields()) { currentNames.add(field.name()); }
        if (previous != null) {
            previous.fields().stream().filter(field -> !currentNames.contains(field.name())).forEach(field -> {
                reservedNumbers.add(field.number()); reservedNames.add(field.name()); unavailable.add(field.number());
            });
        }
        List<PipelineTemplateField> fields = new ArrayList<>();
        for (PipelineTemplateField field : message.fields().stream().sorted(Comparator.comparing(PipelineTemplateField::name)).toList()) {
            Integer number = field.number();
            PipelineIdlSnapshot.FieldSnapshot old = oldByName.get(field.name());
            if (old != null) { number = old.number(); }
            if (number == null) { number = allocator.allocate(unavailable); }
            if (!allocator.isLegal(number) || !unavailable.add(number) && old == null) {
                throw new IllegalStateException("Invalid or duplicate protobuf tag " + number + " for " + message.name() + "." + field.name());
            }
            fields.add(withNumber(field, number));
        }
        return new PipelineTemplateMessage(message.name(), fields,
            new PipelineTemplateReserved(reservedNumbers.stream().distinct().sorted().toList(), reservedNames.stream().distinct().sorted().toList()));
    }

    private PipelineTemplateUnion resolveUnion(PipelineTemplateUnion union, PipelineIdlSnapshot.UnionSnapshot previous) {
        Map<String, PipelineIdlSnapshot.UnionVariantSnapshot> oldByName = new HashMap<>();
        Set<Integer> unavailable = new HashSet<>();
        if (previous != null) { previous.variants().forEach(variant -> { oldByName.put(variant.name(), variant); unavailable.add(variant.number()); }); }
        Map<String, PipelineTemplateUnionVariant> variants = new LinkedHashMap<>();
        for (PipelineTemplateUnionVariant variant : union.variants().values().stream().sorted(Comparator.comparing(PipelineTemplateUnionVariant::name)).toList()) {
            PipelineIdlSnapshot.UnionVariantSnapshot old = oldByName.get(variant.name());
            Integer number = old == null ? variant.number() : old.number();
            if (number == null) { number = allocator.allocate(unavailable); }
            if (!allocator.isLegal(number) || !unavailable.add(number) && old == null) {
                throw new IllegalStateException("Invalid or duplicate protobuf tag " + number + " for " + union.name() + "." + variant.name());
            }
            variants.put(variant.name(), new PipelineTemplateUnionVariant(variant.name(), variant.type(), number));
        }
        return new PipelineTemplateUnion(union.name(), variants);
    }

    private PipelineTemplateField withNumber(PipelineTemplateField field, int number) {
        return new PipelineTemplateField(number, field.name(), field.type(), field.canonicalType(), field.messageRef(),
            field.javaType(), field.protoType(), field.keyType(), field.valueType(), false, field.repeated(), field.deprecated(),
            field.since(), field.deprecatedSince(), field.comment(), field.overrides(), field.referenceable());
    }

    public record Resolved(PipelineTemplateConfig config, PipelineIdlSnapshot state) { }
}
