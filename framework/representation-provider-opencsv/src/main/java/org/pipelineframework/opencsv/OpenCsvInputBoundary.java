package org.pipelineframework.opencsv;

import org.pipelineframework.service.blocking.BlockingIteratorService;

/**
 * Provider-owned marker for an input service that yields OpenCSV rows rather than canonical values.
 * The generated provider facade is the canonical pipeline step; applications implement this boundary contract only.
 */
public interface OpenCsvInputBoundary<I, E> extends BlockingIteratorService<I, E> {

    /**
     * Binds a canonical pipeline input to the provider-owned OpenCSV source representation.
     * Parsing, row enrichment, resource lifecycle, and canonical mapping remain in this provider.
     */
    OpenCsvSource<E> source(I input);

    @Override
    default org.pipelineframework.blocking.CloseableIterator<E> iterateBlocking(I input) {
        return OpenCsvInputBoundarySupport.iterate(source(input));
    }
}
