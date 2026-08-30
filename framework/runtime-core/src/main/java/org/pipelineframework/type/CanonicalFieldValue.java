/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.pipelineframework.type;

import java.util.Objects;
import java.util.Optional;

/** Explicit absent, present-null, or present-value state for generated canonical fields. */
public sealed interface CanonicalFieldValue<T>
    permits CanonicalFieldValue.Absent, CanonicalFieldValue.NullValue, CanonicalFieldValue.Value {

    static <T> CanonicalFieldValue<T> absent() {
        return new Absent<>();
    }

    static <T> CanonicalFieldValue<T> nullValue() {
        return new NullValue<>();
    }

    static <T> CanonicalFieldValue<T> of(T value) {
        return new Value<>(value);
    }

    default boolean isAbsent() {
        return this instanceof Absent<?>;
    }

    default boolean isNull() {
        return this instanceof NullValue<?>;
    }

    Optional<T> asOptional();

    record Absent<T>() implements CanonicalFieldValue<T> {
        @Override public Optional<T> asOptional() { return Optional.empty(); }
    }

    record NullValue<T>() implements CanonicalFieldValue<T> {
        @Override public Optional<T> asOptional() { return Optional.empty(); }
    }

    record Value<T>(T value) implements CanonicalFieldValue<T> {
        public Value {
            Objects.requireNonNull(value, "canonical field value must not be null");
        }

        @Override public Optional<T> asOptional() { return Optional.of(value); }
    }
}
