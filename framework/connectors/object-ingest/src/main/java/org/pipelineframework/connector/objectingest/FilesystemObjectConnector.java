package org.pipelineframework.connector.objectingest;

import java.util.Collection;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectorConcurrencyScope;
import org.pipelineframework.connector.ConnectorExecutionCapabilities;
import org.pipelineframework.connector.ConnectorExecutionStyle;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;

/** Provider packaging for filesystem object source and target operations. */
@ApplicationScoped
public final class FilesystemObjectConnector implements ConnectorProvider<Void> {
    private final FilesystemObjectSourceProvider source = new FilesystemObjectSourceProvider();
    private final FilesystemObjectTargetProvider target = new FilesystemObjectTargetProvider();

    @Override
    public ConnectorProviderId id() {
        return ConnectorProviderId.of("filesystem.objects");
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    /**
     * Declares that the connector provider manages execution style and concurrency.
     *
     * @return the connector's execution capabilities
     */
    @Override
    public ConnectorExecutionCapabilities executionCapabilities() {
        return new ConnectorExecutionCapabilities(
            ConnectorExecutionStyle.PROVIDER_MANAGED, ConnectorConcurrencyScope.PROVIDER_MANAGED);
    }

    /**
     * Provides the filesystem object source and target operations.
     *
     * @return the connector's supported operations
     */
    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(source, target);
    }
}
