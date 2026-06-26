package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Map;
import java.util.Set;

final class ControlPlaneChecks {

    private ControlPlaneChecks() {
    }

    static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    static int requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    static long requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    static <T> List<T> copyList(List<T> value) {
        return value == null ? List.of() : List.copyOf(value);
    }

    static <T> Set<T> copySet(Set<T> value) {
        return value == null ? Set.of() : Set.copyOf(value);
    }

    static <K, V> Map<K, V> copyMap(Map<K, V> value) {
        return value == null ? Map.of() : Map.copyOf(value);
    }
}
