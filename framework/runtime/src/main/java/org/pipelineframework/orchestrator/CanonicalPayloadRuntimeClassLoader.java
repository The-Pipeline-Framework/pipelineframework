package org.pipelineframework.orchestrator;

/**
 * Resolves a generated canonical binding class named in release metadata.
 *
 * <p>Release metadata uses Java source names. Nested generated Java types therefore use dots while the
 * JVM binary name uses dollar signs. The release-selected name is the only input to this resolver; legacy
 * payload class hints never select a class.</p>
 */
public final class CanonicalPayloadRuntimeClassLoader {

    private CanonicalPayloadRuntimeClassLoader() {
    }

    public static Class<?> load(String sourceName, ClassLoader classLoader) throws ClassNotFoundException {
        try {
            return Class.forName(sourceName, false, classLoader);
        } catch (ClassNotFoundException firstFailure) {
            String candidate = sourceName;
            for (int index = candidate.lastIndexOf('.'); index >= 0; index = candidate.lastIndexOf('.', index - 1)) {
                candidate = candidate.substring(0, index) + "$" + candidate.substring(index + 1);
                try {
                    return Class.forName(candidate, false, classLoader);
                } catch (ClassNotFoundException ignored) {
                    // Continue replacing enclosing source-name separators from right to left.
                }
            }
            throw firstFailure;
        }
    }
}
