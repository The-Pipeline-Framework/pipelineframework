package org.pipelineframework.processor.phase;

/**
 * Handles naming policies and transformations for various identifiers.
 */
class NamingPolicy {

    /**
     * Converts input to PascalCase.
     *
     * @param input the input string
     * @return the PascalCase version of the input
     */
    static String toPascalCase(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }
        StringBuilder builder = new StringBuilder();
        String[] parts = input.split("[^a-zA-Z0-9]+");
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase());
            if (part.length() > 1) {
                builder.append(part.substring(1));
            }
        }
        return builder.toString();
    }

    /**
     * Strips the "Process " prefix from a name.
     *
     * @param name the name to strip
     * @return the name without the prefix
     */
    static String stripProcessPrefix(String name) {
        if (name == null) {
            return "";
        }
        if (name.startsWith("Process ")) {
            return name.substring("Process ".length());
        }
        return name;
    }

    /**
     * Formats a string for use as a class name.
     *
     * @param input the input string
     * @return the formatted class name
     */
    static String formatForClassName(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        String[] parts = input.split(" ");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            builder.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                builder.append(lower.substring(1));
            }
        }
        return builder.toString();
    }

    /**
     * Converts an empty string to null.
     *
     * @param value the value to check
     * @return the value or null if it's empty
     */
    static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}