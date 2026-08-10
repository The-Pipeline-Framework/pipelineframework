package org.pipelineframework.opencsv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import java.io.Reader;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourcePage;

class OpenCsvResumableSourceSupportTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void resumesAtLogicalRecordsAcrossUtf8MultilineAndParserBuffering() throws Exception {
        Path csv = temporaryDirectory.resolve("payments.csv");
        Files.writeString(csv, "ID,Recipient,Note\n"
            + "1,José,\"first line\nsecond 😀 line\"\n"
            + "2,Åsa,plain\n"
            + "3,李雷,\"last\nrecord\"\n", StandardCharsets.UTF_8);
        TestSource source = new TestSource(csv);

        ResumableSourcePage<CanonicalRow> first = OpenCsvResumableSourceSupport
            .readPage(source, MAPPER, OpaqueSourceCheckpoint.initial(), 2).await().indefinitely();
        ResumableSourcePage<CanonicalRow> second = OpenCsvResumableSourceSupport
            .readPage(source, MAPPER, first.nextCheckpoint(), 2).await().indefinitely();

        assertEquals(List.of(
            new CanonicalRow("1", "José", "first line\nsecond 😀 line", source.sourceName()),
            new CanonicalRow("2", "Åsa", "plain", source.sourceName())), first.items());
        assertEquals(List.of(new CanonicalRow("3", "李雷", "last\nrecord", source.sourceName())), second.items());
        assertEquals(false, first.endOfSource());
        assertEquals(true, second.endOfSource());
    }

    @Test
    void rejectsCheckpointForDifferentSource() throws Exception {
        Path first = temporaryDirectory.resolve("first.csv");
        Path second = temporaryDirectory.resolve("second.csv");
        Files.writeString(first, "ID,Recipient,Note\n1,A,one\n", StandardCharsets.UTF_8);
        Files.writeString(second, "ID,Recipient,Note\n1,A,one\n", StandardCharsets.UTF_8);
        ResumableSourcePage<CanonicalRow> page = OpenCsvResumableSourceSupport
            .readPage(new TestSource(first), MAPPER, OpaqueSourceCheckpoint.initial(), 1).await().indefinitely();

        assertThrows(IllegalStateException.class, () -> OpenCsvResumableSourceSupport
            .readPage(new TestSource(second), MAPPER, page.nextCheckpoint(), 1).await().indefinitely());
    }

    private static final Mapper<CanonicalRow, ParsedRow> MAPPER = new Mapper<>() {
        @Override
        public CanonicalRow fromExternal(ParsedRow external) {
            return new CanonicalRow(external.id, external.recipient, external.note, external.source);
        }

        @Override
        public ParsedRow toExternal(CanonicalRow domain) {
            ParsedRow row = new ParsedRow();
            row.id = domain.id();
            row.recipient = domain.recipient();
            row.note = domain.note();
            row.source = domain.source();
            return row;
        }
    };

    private record CanonicalRow(String id, String recipient, String note, String source) {
    }

    public static final class ParsedRow {
        @CsvBindByName(column = "ID")
        String id;
        @CsvBindByName(column = "Recipient")
        String recipient;
        @CsvBindByName(column = "Note")
        String note;
        String source;
    }

    private static final class TestSource implements ResumableOpenCsvSource<ParsedRow> {
        private final Path path;

        private TestSource(Path path) {
            this.path = path;
        }

        @Override
        public Reader openReader() throws java.io.IOException {
            return Files.newBufferedReader(path, StandardCharsets.UTF_8);
        }

        @Override
        public SeekableByteChannel openSeekableChannel() throws java.io.IOException {
            return Files.newByteChannel(path);
        }

        @Override
        public String resumableSourceId() {
            return path.toAbsolutePath().normalize().toString();
        }

        @Override
        public Class<ParsedRow> rowType() {
            return ParsedRow.class;
        }

        @Override
        public HeaderColumnNameMappingStrategy<ParsedRow> mappingStrategy() {
            HeaderColumnNameMappingStrategy<ParsedRow> strategy = new HeaderColumnNameMappingStrategy<>();
            strategy.setType(ParsedRow.class);
            return strategy;
        }

        @Override
        public void enrichParsedRow(ParsedRow row) {
            row.source = sourceName();
        }

        @Override
        public String sourceName() {
            return path.toString();
        }
    }
}
