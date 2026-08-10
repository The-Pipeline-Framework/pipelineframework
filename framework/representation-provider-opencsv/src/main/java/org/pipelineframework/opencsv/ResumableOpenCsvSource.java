package org.pipelineframework.opencsv;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/** Provider-facing source capability for logical-record resume; it exposes no cursor to authored services. */
public interface ResumableOpenCsvSource<E> extends OpenCsvSource<E> {

    SeekableByteChannel openSeekableChannel() throws IOException;

    /** Stable provider-local identity checked before a persisted checkpoint is resumed. */
    String resumableSourceId();
}
