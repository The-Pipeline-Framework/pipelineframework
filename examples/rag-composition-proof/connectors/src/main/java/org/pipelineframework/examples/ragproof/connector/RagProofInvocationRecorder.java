package org.pipelineframework.examples.ragproof.connector;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

/** Test evidence that capture and effect replay avoid provider redispatch. */
@ApplicationScoped
public class RagProofInvocationRecorder {
    private final AtomicInteger embeddings = new AtomicInteger();
    private final AtomicInteger upserts = new AtomicInteger();
    private final AtomicInteger searches = new AtomicInteger();
    private final AtomicInteger answers = new AtomicInteger();

    void embedding() { embeddings.incrementAndGet(); }
    void upsert() { upserts.incrementAndGet(); }
    void search() { searches.incrementAndGet(); }
    void answer() { answers.incrementAndGet(); }

    public int embeddingCount() { return embeddings.get(); }
    public int upsertCount() { return upserts.get(); }
    public int searchCount() { return searches.get(); }
    public int answerCount() { return answers.get(); }

    public void reset() {
        embeddings.set(0);
        upserts.set(0);
        searches.set(0);
        answers.set(0);
    }
}
