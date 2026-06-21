package org.pipelineframework.objectpublish;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * Provider-neutral streaming object write session.
 */
public interface ObjectWriteSession {
    CompletionStage<Void> write(ByteBuffer chunk);

    CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest request);

    CompletionStage<Void> abort(Throwable cause);
}
