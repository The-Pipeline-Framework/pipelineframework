package org.pipelineframework.objectpublish;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import org.pipelineframework.connector.ObjectTargetOperation;

/**
 * Provider SPI for Object Publish targets.
 */
public interface ObjectTargetProvider extends ObjectTargetOperation {
    String providerName();

    @Override
    default String id() {
        return providerName();
    }

    default CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
        ObjectWriteOpenRequest openRequest = new ObjectWriteOpenRequest(
            request.targetName(),
            request.target(),
            request.objectKey(),
            request.contentType(),
            request.metadata(),
            request.idempotencyKey());
        return open(openRequest)
            .thenCompose(session -> session.write(ByteBuffer.wrap(request.bytes()))
                .thenCompose(ignored -> session.close(new ObjectWriteCloseRequest(
                    request.bytes().length,
                    request.checksum(),
                    request.metadata()))));
    }

    CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request);
}
