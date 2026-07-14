package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;

/**
 * One-shot, EOF-delimited object source backed by standard input.
 */
public final class StdioObjectSourceProvider implements ObjectSourceProvider {
    private static final String ENDPOINT = "stdin";
    private final StandardStreams streams;
    private Optional<CapturedInput> captured = Optional.empty();
    private boolean listed;

    public StdioObjectSourceProvider() {
        this(StandardStreams.jvm());
    }

    StdioObjectSourceProvider(StandardStreams streams) {
        this.streams = Objects.requireNonNull(streams, "streams");
    }

    @Override
    public String providerName() {
        return "stdio";
    }

    @Override
    public synchronized List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
        validate(source);
        if (listed || limit <= 0) {
            return List.of();
        }
        CapturedInput input = capture(source);
        listed = true;
        captured = Optional.of(input);
        return List.of(input.item(source.name()));
    }

    @Override
    public synchronized Optional<String> readText(PipelineObjectSourceConfig source, ObjectSourceItem item, long maxBytes) {
        validate(source);
        CapturedInput input = captured.orElseThrow(() -> new IllegalStateException("stdin has not been listed"));
        if (!ENDPOINT.equals(item.key())) {
            throw new IllegalArgumentException("stdio stdin object key must be 'stdin'");
        }
        if (maxBytes > 0 && input.bytes().length > maxBytes) {
            throw new IllegalStateException("stdin exceeds configured maxBytes");
        }
        return Optional.of(new String(input.bytes(), source.payload().charset()));
    }

    private CapturedInput capture(PipelineObjectSourceConfig source) {
        long maxBytes = source.payload().maxBytes();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = streams.stdin().read(buffer)) != -1) {
                if (maxBytes > 0 && output.size() + read > maxBytes) {
                    throw new IllegalStateException("stdin exceeds configured maxBytes");
                }
                output.write(buffer, 0, read);
            }
            byte[] bytes = output.toByteArray();
            return new CapturedInput(bytes, checksum(bytes));
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading stdin object source", e);
        }
    }

    private void validate(PipelineObjectSourceConfig source) {
        if (!"text".equalsIgnoreCase(source.payload().mode())) {
            throw new IllegalArgumentException("stdio stdin source requires payload.mode=text");
        }
        if (!Map.of("endpoint", ENDPOINT).equals(source.location())) {
            throw new IllegalArgumentException("stdio stdin source requires location.endpoint=stdin and no filesystem options");
        }
    }

    private static String checksum(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    private record CapturedInput(byte[] bytes, String checksum) {
        private ObjectSourceItem item(String sourceName) {
            return new ObjectSourceItem(
                "stdio", ENDPOINT, ENDPOINT, null, checksum, bytes.length, 0L, "text/plain",
                Map.of("source", sourceName, "endpoint", ENDPOINT), null, null);
        }
    }
}
