package org.pipelineframework.connector.objectingest;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectorCompletionStages;
import org.pipelineframework.connector.ConnectorConcurrencyScope;
import org.pipelineframework.connector.ConnectorExecutionCapabilities;
import org.pipelineframework.connector.ConnectorExecutionStyle;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;

/** Provider packaging and lifecycle for S3 object source and target operations. */
@ApplicationScoped
public final class S3ObjectConnector implements ConnectorProvider<Void> {
    private final S3ObjectSourceProvider source = new S3ObjectSourceProvider();
    private final S3ObjectTargetProvider target = new S3ObjectTargetProvider();

    @Override
    public ConnectorProviderId id() {
        return ConnectorProviderId.of("s3.objects");
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public ConnectorExecutionCapabilities executionCapabilities() {
        return new ConnectorExecutionCapabilities(
            ConnectorExecutionStyle.PROVIDER_MANAGED, ConnectorConcurrencyScope.PROVIDER_MANAGED);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(source, target);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        source.close();
        target.close();
        return ConnectorCompletionStages.completed();
    }
}
