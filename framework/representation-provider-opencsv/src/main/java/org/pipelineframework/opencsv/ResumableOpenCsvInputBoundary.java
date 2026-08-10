package org.pipelineframework.opencsv;

/**
 * Opt-in marker for an OpenCSV source whose representation can reopen at a provider-owned logical
 * record checkpoint. Applications still bind only their canonical input to {@link #source(Object)};
 * cursor handling remains entirely inside the OpenCSV provider.
 */
public interface ResumableOpenCsvInputBoundary<I, E> extends OpenCsvInputBoundary<I, E> {
}
