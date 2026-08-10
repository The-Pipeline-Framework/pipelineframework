package org.pipelineframework.opencsv;

import com.opencsv.bean.CsvToBeanBuilder;
import java.io.Reader;
import java.util.Iterator;
import org.jboss.logging.Logger;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.mapper.Mapper;

/** Provider-owned live OpenCSV parsing and external-to-canonical conversion path. */
public final class OpenCsvInputBoundarySupport {
    private static final Logger LOG = Logger.getLogger(OpenCsvInputBoundarySupport.class);

    private OpenCsvInputBoundarySupport() {
    }

    public static <E> CloseableIterator<E> iterate(OpenCsvSource<E> source) {
        try {
            Reader reader = source.openReader();
            try {
                Iterator<E> rows = new CsvToBeanBuilder<E>(reader)
                    .withType(source.rowType())
                    .withMappingStrategy(source.mappingStrategy())
                    .withSeparator(',')
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build()
                    .iterator();
                return new ParsedRowIterator<>(reader, rows, source);
            } catch (Exception exception) {
                reader.close();
                throw exception;
            }
        } catch (Exception exception) {
            LOG.errorf(exception, "CSV processing failed for file: %s", source.sourceName());
            throw new RuntimeException("CSV processing error: " + exception.getMessage(), exception);
        }
    }

    public static <D, E> CloseableIterator<D> iterateAndMap(OpenCsvSource<E> source, Mapper<D, E> mapper) {
        return new MappingIterator<>(iterate(source), mapper);
    }

    private static final class ParsedRowIterator<E> implements CloseableIterator<E> {
        private final Reader reader;
        private final Iterator<E> rows;
        private final OpenCsvSource<E> source;
        private long emitted;
        private boolean closed;

        private ParsedRowIterator(Reader reader, Iterator<E> rows, OpenCsvSource<E> source) {
            this.reader = reader;
            this.rows = rows;
            this.source = source;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public E next() {
            E row = rows.next();
            source.enrichParsedRow(row);
            emitted++;
            if (LOG.isDebugEnabled()) {
                LOG.debugf("Executed blocking CSV iteration on %s", source.sourceName());
            }
            return row;
        }

        @Override
        public void close() throws Exception {
            if (closed) {
                return;
            }
            closed = true;
            reader.close();
            LOG.infof("Closed CSV reader for: %s (iterated %d records)", source.sourceName(), emitted);
        }
    }

    private static final class MappingIterator<D, E> implements CloseableIterator<D> {
        private final CloseableIterator<E> rows;
        private final Mapper<D, E> mapper;

        private MappingIterator(CloseableIterator<E> rows, Mapper<D, E> mapper) {
            this.rows = rows;
            this.mapper = mapper;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public D next() {
            return mapper.fromExternal(rows.next());
        }

        @Override
        public void close() throws Exception {
            rows.close();
        }
    }
}
