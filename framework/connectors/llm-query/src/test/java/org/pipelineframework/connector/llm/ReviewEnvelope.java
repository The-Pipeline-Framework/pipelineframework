package org.pipelineframework.connector.llm;

public record ReviewEnvelope(String invoiceId, ReviewReady review) { }
