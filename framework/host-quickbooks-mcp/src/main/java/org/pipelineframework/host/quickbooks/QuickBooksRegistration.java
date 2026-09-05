package org.pipelineframework.host.quickbooks;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.pipelineframework.connector.ConnectionRef;

/** Local host-attested binding to an opaque Node instance. Never pipeline data or credential state. */
public record QuickBooksRegistration(
    String tenantId, ConnectionRef reference, String serverInstanceId,
    List<String> command, Path workingDirectory
) {
    public QuickBooksRegistration {
        if (Objects.requireNonNull(tenantId).isBlank() || Objects.requireNonNull(serverInstanceId).isBlank()) {
            throw new IllegalArgumentException("Invalid local MCP registration identity");
        }
        Objects.requireNonNull(reference);
        command = List.copyOf(command);
        if (command.isEmpty() || !Path.of(command.getFirst()).isAbsolute()
            || !Objects.requireNonNull(workingDirectory).isAbsolute()) {
            throw new IllegalArgumentException("MCP executable and working directory must be absolute");
        }
        workingDirectory = workingDirectory.normalize();
    }

    @Override
    public String toString() {
        return "QuickBooksRegistration[host-owned]";
    }
}
