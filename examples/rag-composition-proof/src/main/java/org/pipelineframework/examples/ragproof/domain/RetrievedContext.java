package org.pipelineframework.examples.ragproof.domain;

import java.util.List;

public record RetrievedContext(String questionId, String question, List<String> passages) {
    public RetrievedContext {
        passages = List.copyOf(passages);
    }
}
