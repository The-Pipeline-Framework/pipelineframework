package org.pipelineframework.opencsv;

import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import java.io.IOException;
import java.io.Reader;

/**
 * Provider-facing description of one OpenCSV source representation.
 *
 * <p>The representation supplies its reader and row mapping semantics; the provider owns parser
 * construction, iterator lifecycle, and the point at which a parsed row is enriched before it is
 * converted to the canonical value.
 */
public interface OpenCsvSource<E> {

    Reader openReader() throws IOException;

    Class<E> rowType();

    HeaderColumnNameMappingStrategy<E> mappingStrategy();

    void enrichParsedRow(E row);

    String sourceName();
}
