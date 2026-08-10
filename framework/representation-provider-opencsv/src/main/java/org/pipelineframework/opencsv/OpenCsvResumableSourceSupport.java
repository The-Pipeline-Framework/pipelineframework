package org.pipelineframework.opencsv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourceDescriptor;
import org.pipelineframework.stream.ResumableSourcePage;

/** Provider-owned OpenCSV logical-record paging. Checkpoints are UTF-8 byte positions, never lines. */
public final class OpenCsvResumableSourceSupport {
    private static final String CHECKPOINT_VERSION = "opencsv-logical-v1";

    private OpenCsvResumableSourceSupport() {
    }

    public static ResumableSourceDescriptor descriptor(String generatedFacadeType) {
        return new ResumableSourceDescriptor("opencsv", generatedFacadeType, CHECKPOINT_VERSION);
    }

    public static <D, E> Uni<ResumableSourcePage<D>> readPage(
        OpenCsvSource<E> source,
        Mapper<D, E> mapper,
        OpaqueSourceCheckpoint checkpoint,
        int limit
    ) {
        return Uni.createFrom().item(() -> {
            try {
                return readPageBlocking(requireResumable(source), mapper, checkpoint, limit);
            } catch (Exception exception) {
                throw new IllegalStateException("OpenCSV resumable page failed", exception);
            }
        });
    }

    private static <D, E> ResumableSourcePage<D> readPageBlocking(
        ResumableOpenCsvSource<E> source,
        Mapper<D, E> mapper,
        OpaqueSourceCheckpoint checkpoint,
        int limit
    ) throws Exception {
        if (limit <= 0) {
            throw new IllegalArgumentException("OpenCSV resumable page limit must be positive");
        }
        Objects.requireNonNull(mapper, "mapper must not be null");
        String sourceId = source.resumableSourceId();
        long offset = checkpoint.value().map(value -> decodeCheckpoint(value, sourceId)).orElse(-1L);
        HeaderColumnNameMappingStrategy<E> strategy = source.mappingStrategy();
        long headerEnd = captureHeader(source, strategy);
        long start = offset < 0 ? headerEnd : offset;
        if (start < headerEnd) {
            throw new IllegalStateException("OpenCSV checkpoint precedes the logical data region");
        }
        List<D> items = new ArrayList<>(limit);
        long nextOffset = start;
        boolean endOfSource;
        try (SeekableByteChannel channel = source.openSeekableChannel()) {
            channel.position(start);
            try (CSVReader reader = csvReader(channel)) {
                for (int count = 0; count < limit; count++) {
                    String[] row = reader.readNext();
                    if (row == null) {
                        return new ResumableSourcePage<>(List.copyOf(items),
                            new OpaqueSourceCheckpoint(Optional.of(encodeCheckpoint(sourceId, nextOffset))), true);
                    }
                    E parsed = strategy.populateNewBean(row);
                    source.enrichParsedRow(parsed);
                    items.add(mapper.fromExternal(parsed));
                    nextOffset = channel.position();
                }
                // Bounded one-record lookahead determines sealing. The persisted checkpoint remains
                // before that record, so re-opening re-parses it rather than losing buffered input.
                endOfSource = reader.readNext() == null;
            }
        }
        return new ResumableSourcePage<>(List.copyOf(items),
            new OpaqueSourceCheckpoint(Optional.of(encodeCheckpoint(sourceId, nextOffset))), endOfSource);
    }

    private static <E> long captureHeader(ResumableOpenCsvSource<E> source, HeaderColumnNameMappingStrategy<E> strategy)
        throws Exception {
        try (SeekableByteChannel channel = source.openSeekableChannel(); CSVReader reader = csvReader(channel)) {
            strategy.captureHeader(reader);
            return channel.position();
        }
    }

    private static CSVReader csvReader(SeekableByteChannel channel) {
        return new CSVReaderBuilder(new Utf8CheckpointReader(channel)).build();
    }

    @SuppressWarnings("unchecked")
    private static <E> ResumableOpenCsvSource<E> requireResumable(OpenCsvSource<E> source) {
        if (!(source instanceof ResumableOpenCsvSource<?> resumable)) {
            throw new IllegalStateException("OpenCSV resumable capability requires ResumableOpenCsvSource");
        }
        return (ResumableOpenCsvSource<E>) resumable;
    }

    private static String encodeCheckpoint(String sourceId, long offset) {
        String encodedSource = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(sourceId.getBytes(StandardCharsets.UTF_8));
        return CHECKPOINT_VERSION + ":" + encodedSource + ":" + offset;
    }

    private static long decodeCheckpoint(String checkpoint, String expectedSourceId) {
        String[] fields = checkpoint.split(":", 3);
        if (fields.length != 3 || !CHECKPOINT_VERSION.equals(fields[0])) {
            throw new IllegalStateException("Unsupported OpenCSV logical-record checkpoint");
        }
        String sourceId = new String(Base64.getUrlDecoder().decode(fields[1]), StandardCharsets.UTF_8);
        if (!expectedSourceId.equals(sourceId)) {
            throw new IllegalStateException("OpenCSV checkpoint belongs to a different source");
        }
        try {
            long offset = Long.parseLong(fields[2]);
            if (offset < 0) {
                throw new IllegalStateException("OpenCSV checkpoint offset must not be negative");
            }
            return offset;
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Invalid OpenCSV logical-record checkpoint", exception);
        }
    }

    /**
     * Deliberately serves one decoded character per read. OpenCSV may buffer characters, but it
     * cannot advance the seekable channel past a completed logical record behind the provider's
     * back. UTF-8 is decoded as complete code points, including supplementary characters.
     */
    private static final class Utf8CheckpointReader extends Reader {
        private final SeekableByteChannel channel;
        private String pending = "";
        private boolean closed;

        private Utf8CheckpointReader(SeekableByteChannel channel) {
            this.channel = Objects.requireNonNull(channel, "channel must not be null");
        }

        @Override
        public int read(char[] target, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("reader is closed");
            }
            if (length == 0) {
                return 0;
            }
            if (pending.isEmpty()) {
                pending = nextCodePoint();
            }
            if (pending.isEmpty()) {
                return -1;
            }
            target[offset] = pending.charAt(0);
            pending = pending.substring(1);
            return 1;
        }

        private String nextCodePoint() throws IOException {
            ByteBuffer first = ByteBuffer.allocate(1);
            if (channel.read(first) < 0) {
                return "";
            }
            int lead = first.array()[0] & 0xff;
            int size = lead < 0x80 ? 1 : (lead & 0xe0) == 0xc0 ? 2 : (lead & 0xf0) == 0xe0 ? 3 : 4;
            byte[] bytes = new byte[size];
            bytes[0] = (byte) lead;
            for (int index = 1; index < size; index++) {
                ByteBuffer next = ByteBuffer.allocate(1);
                if (channel.read(next) < 0) {
                    throw new IOException("Truncated UTF-8 sequence in OpenCSV source");
                }
                bytes[index] = next.array()[0];
            }
            String decoded = new String(bytes, StandardCharsets.UTF_8);
            if (decoded.indexOf('\ufffd') >= 0) {
                throw new IOException("Invalid UTF-8 sequence in OpenCSV source");
            }
            return decoded;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
