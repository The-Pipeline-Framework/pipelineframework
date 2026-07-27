package org.pipelineframework.representation.spi;

/** A deterministic, provider-owned validation or capability diagnostic. */
public record ProviderDiagnostic(Severity severity, String code, String message) {
    public enum Severity { ERROR, WARNING }

    public ProviderDiagnostic {
        if (severity == null || code == null || code.isBlank() || message == null || message.isBlank()) {
            throw new IllegalArgumentException("diagnostic severity, code, and message must be present");
        }
        code = code.trim();
        message = message.trim();
    }
}
