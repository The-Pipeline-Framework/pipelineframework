package org.pipelineframework.opencsv;

import org.pipelineframework.service.blocking.BlockingIteratorService;

/**
 * Provider-owned marker for an input service that yields OpenCSV rows rather than canonical values.
 * The generated provider facade is the canonical pipeline step; applications implement this boundary contract only.
 */
public interface OpenCsvInputBoundary<I, E> extends BlockingIteratorService<I, E> {
}
