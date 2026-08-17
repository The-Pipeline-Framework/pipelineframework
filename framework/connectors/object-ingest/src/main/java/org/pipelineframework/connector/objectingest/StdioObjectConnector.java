package org.pipelineframework.connector.objectingest;

import java.util.Collection;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;

/** Provider packaging for standard-stream object source and target operations. */
@ApplicationScoped
public final class StdioObjectConnector implements ConnectorProvider<Void> {
    private final StandardStreams streams;

    public StdioObjectConnector() {
        this(StandardStreams.jvm());
    }

    StdioObjectConnector(StandardStreams streams) {
        this.streams = streams;
    }

    @Override
    public ConnectorProviderId id() {
        return ConnectorProviderId.of("stdio.objects");
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(new StdioObjectSourceProvider(streams), new StdioObjectTargetProvider(streams));
    }
}
