package org.pipelineframework.connector.gmail;

import java.util.Objects;

import com.google.api.services.gmail.Gmail;
import org.pipelineframework.connector.ResolvedConnection;

/** Runtime-only Gmail SDK client whose authentication lifecycle remains owned by the host. */
public record AuthenticatedGmailConnection(Gmail client) implements ResolvedConnection {
    public AuthenticatedGmailConnection {
        client = Objects.requireNonNull(client, "authenticated Gmail client must not be null");
    }
}
