package org.pipelineframework.connector.objectingest;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;

/** Provider packaging and lifecycle for S3 object source and target operations. */
@ApplicationScoped
public final class S3ObjectConnector implements ConnectorProvider<Void> {
    private final S3ObjectSourceProvider source;
    private final S3ObjectTargetProvider target;

    public S3ObjectConnector() {
        this(new S3ObjectSourceProvider(), new S3ObjectTargetProvider());
    }

    S3ObjectConnector(S3ObjectSourceProvider source, S3ObjectTargetProvider target) {
        this.source = source;
        this.target = target;
    }

    @Override
    public ConnectorProviderId id() {
        return ConnectorProviderId.of("s3.objects");
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(source, target);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        return source.stop().thenCompose(ignored -> {
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            Thread.ofVirtual().name("tpf-s3-target-stop").start(() -> {
                try {
                    target.close();
                    stopped.complete(null);
                } catch (Throwable failure) {
                    stopped.completeExceptionally(failure);
                }
            });
            return stopped;
        });
    }
}
