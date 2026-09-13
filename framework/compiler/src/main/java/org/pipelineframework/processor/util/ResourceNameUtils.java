package org.pipelineframework.processor.util;

/**
 * Helpers for deriving stable resource/model names from generated class names.
 */
public final class ResourceNameUtils {

    private static final String SERVICE_SUFFIX = "Service";
    private static final String REACTIVE_SUFFIX = "Reactive";

    private ResourceNameUtils() {
    }

    /**
     * Normalize a raw generated name by trimming trailing "Service" and "Reactive" suffixes.
     * Falls back to "Unnamed" when the input is null/blank or becomes empty after trimming.
     */
    public static String normalizeBaseName(String rawGeneratedName) {
        if (rawGeneratedName == null || rawGeneratedName.isBlank()) {
            return "Unnamed";
        }
        String baseName = rawGeneratedName;
        if (baseName.endsWith(SERVICE_SUFFIX)) {
            baseName = baseName.substring(0, baseName.length() - SERVICE_SUFFIX.length());
        }
        if (baseName.endsWith(REACTIVE_SUFFIX)) {
            baseName = baseName.substring(0, baseName.length() - REACTIVE_SUFFIX.length());
        }
        return baseName.isBlank() ? "Unnamed" : baseName;
    }
}
