package org.pipelineframework.examples.quickbooks.domain;

import java.util.Objects;

/** Java representation of the imported canonical MCP request contract. */
public record QuickBooksAgedReceivablesRequest(QuickBooksAgedReceivablesRequestParams params) {
    public QuickBooksAgedReceivablesRequest {
        Objects.requireNonNull(params, "params");
    }
}
