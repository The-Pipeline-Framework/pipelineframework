package org.pipelineframework.connector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * CompletionStage helpers that keep the public SPI on JDK async types.
 */
public final class ConnectorCompletionStages {
    private ConnectorCompletionStages() {
    }

    public static CompletionStage<Void> completed() {
        return CompletableFuture.runAsync(() -> {
        }, Runnable::run);
    }
}
