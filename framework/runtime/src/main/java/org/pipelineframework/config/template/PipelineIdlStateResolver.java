/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.config.template;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves compiler-owned protobuf tags from persisted IDL state. */
public final class PipelineIdlStateResolver {

    private final PipelineIdlTagAllocator allocator = new PipelineIdlTagAllocator();

    public Resolved resolve(PipelineTemplateConfig config, PipelineIdlSnapshot baseline, boolean bootstrap) {
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
