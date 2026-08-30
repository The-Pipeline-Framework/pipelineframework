package org.pipelineframework.connector.gmail;

import java.util.Objects;

import org.pipelineframework.connector.ConnectionRef;

/** Deployment-owned logical connection binding. It never contains credentials or account IDs. */
public record GmailProviderConfiguration(ConnectionRef connection) {
    public GmailProviderConfiguration {
        connection = Objects.requireNonNull(connection, "Gmail connection reference must not be null");
    }
}
