package org.pipelineframework.connector.openapi.maven;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/** Bounded acquisition and local rewriting of one explicitly authorized HTTPS contract closure. */
final class OpenApiAcquisition {
    static final int MAX_DOCUMENTS = 64;
    static final int MAX_DOCUMENT_BYTES = 4 * 1024 * 1024;
    static final long MAX_CLOSURE_BYTES = 16L * 1024 * 1024;
    static final int MAX_REFERENCE_DEPTH = 32;
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();

    private OpenApiAcquisition() {
    }

    static Acquisition acquire(
        URI source,
        Path snapshot,
        Set<String> configuredAllowedOrigins,
        Fetcher fetcher
    ) throws IOException, InterruptedException {
        URI root = validateDocumentUri(source, "OpenAPI acquisition source");
        if (root.getFragment() != null) {
            throw new IllegalArgumentException("OpenAPI acquisition source must not contain a fragment");
        }
        Path target = Objects.requireNonNull(snapshot, "OpenAPI snapshot must not be null")
            .toAbsolutePath().normalize();
        Set<String> allowedOrigins = new LinkedHashSet<>();
        allowedOrigins.add(origin(root));
        Objects.requireNonNull(configuredAllowedOrigins, "allowed origins must not be null").stream()
            .map(value -> validateOrigin(URI.create(value)))
            .forEach(allowedOrigins::add);
        Map<URI, Document> documents = new LinkedHashMap<>();
        collect(root, 0, allowedOrigins, Objects.requireNonNull(fetcher, "OpenAPI fetcher must not be null"), documents,
            new LinkedHashSet<>());
        Map<URI, Path> destinations = destinations(root, target, documents.keySet());
        for (Map.Entry<URI, Document> entry : documents.entrySet()) {
            JsonNode rewritten = entry.getValue().node().deepCopy();
            rewriteReferences(rewritten, entry.getKey(), destinations.get(entry.getKey()), destinations);
            atomicWrite(destinations.get(entry.getKey()), YAML.writeValueAsBytes(rewritten));
        }
        String closureDigest = digest(documents);
        List<AcquiredDocument> acquired = documents.entrySet().stream()
            .sorted(Map.Entry.comparingByKey(Comparator.comparing(URI::toASCIIString)))
            .map(entry -> new AcquiredDocument(entry.getKey().toASCIIString(), sha256(entry.getValue().bytes()),
                target.getParent().relativize(destinations.get(entry.getKey())).toString().replace('\\', '/')))
            .toList();
        Acquisition result = new Acquisition(root.toASCIIString(), sha256(documents.get(root).bytes()),
            closureDigest, acquired);
        atomicWrite(manifest(target), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(result));
        return result;
    }

    static Path manifest(Path snapshot) {
        return snapshot.resolveSibling(snapshot.getFileName() + ".acquisition.json");
    }

    private static void collect(
        URI uri,
        int depth,
        Set<String> allowedOrigins,
        Fetcher fetcher,
        Map<URI, Document> documents,
        Set<URI> active
    ) throws IOException, InterruptedException {
        if (depth > MAX_REFERENCE_DEPTH) throw new IllegalArgumentException("OpenAPI acquisition reference depth exceeds limit");
        URI documentUri = withoutFragment(validateDocumentUri(uri, "OpenAPI reference"));
        if (!allowedOrigins.contains(origin(documentUri))) {
            throw new IllegalArgumentException("OpenAPI reference origin is not explicitly allowed: " + origin(documentUri));
        }
        if (active.contains(documentUri)) {
            throw new IllegalArgumentException("OpenAPI acquisition reference closure contains a cycle at " + documentUri);
        }
        if (documents.containsKey(documentUri)) return;
        if (documents.size() >= MAX_DOCUMENTS) {
            throw new IllegalArgumentException("OpenAPI acquisition reference closure exceeds document limit");
        }
        byte[] bytes = Objects.requireNonNull(fetcher.fetch(documentUri), "OpenAPI fetcher returned null bytes");
        if (bytes.length > MAX_DOCUMENT_BYTES) throw new IllegalArgumentException("OpenAPI document exceeds byte limit");
        long closureBytes = documents.values().stream().mapToLong(value -> value.bytes().length).sum() + bytes.length;
        if (closureBytes > MAX_CLOSURE_BYTES) throw new IllegalArgumentException("OpenAPI acquisition closure exceeds byte limit");
        JsonNode node = YAML.readTree(bytes);
        if (node == null || !node.isObject()) throw new IllegalArgumentException("OpenAPI acquired document must be an object");
        documents.put(documentUri, new Document(bytes.clone(), node));
        active.add(documentUri);
        List<String> references = new ArrayList<>();
        collectReferences(node, references);
        for (String reference : references.stream().sorted().toList()) {
            String resource = reference.split("#", 2)[0];
            if (resource.isBlank()) continue;
            collect(documentUri.resolve(resource), depth + 1, allowedOrigins, fetcher, documents, active);
        }
        active.remove(documentUri);
    }

    private static Map<URI, Path> destinations(URI root, Path target, Set<URI> sources) {
        Map<URI, Path> result = new LinkedHashMap<>();
        result.put(root, target);
        Path referenceDirectory = target.resolveSibling(target.getFileName() + ".refs");
        sources.stream().filter(source -> !source.equals(root)).sorted(Comparator.comparing(URI::toASCIIString))
            .forEach(source -> result.put(source, referenceDirectory.resolve(sha256(
                source.toASCIIString().getBytes(StandardCharsets.UTF_8)) + extension(source))));
        return result;
    }

    private static void rewriteReferences(
        JsonNode node,
        URI source,
        Path localSource,
        Map<URI, Path> destinations
    ) {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            object.fields().forEachRemaining(entry -> {
                if ("$ref".equals(entry.getKey()) && entry.getValue().isTextual()) {
                    String reference = entry.getValue().textValue();
                    String[] split = reference.split("#", 2);
                    if (!split[0].isBlank()) {
                        URI targetUri = withoutFragment(source.resolve(split[0]));
                        Path localTarget = destinations.get(targetUri);
                        if (localTarget == null) throw new IllegalArgumentException("OpenAPI reference was not acquired: " + targetUri);
                        String relative = localSource.getParent().relativize(localTarget).toString().replace('\\', '/');
                        object.put(entry.getKey(), relative + (split.length == 2 ? "#" + split[1] : ""));
                    }
                } else {
                    rewriteReferences(entry.getValue(), source, localSource, destinations);
                }
            });
        } else if (node.isArray()) {
            node.forEach(value -> rewriteReferences(value, source, localSource, destinations));
        }
    }

    private static void collectReferences(JsonNode node, List<String> references) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                if ("$ref".equals(entry.getKey()) && entry.getValue().isTextual()) references.add(entry.getValue().textValue());
                else collectReferences(entry.getValue(), references);
            });
        } else if (node.isArray()) {
            node.forEach(value -> collectReferences(value, references));
        }
    }

    private static URI validateDocumentUri(URI uri, String label) {
        URI value = Objects.requireNonNull(uri, label + " must not be null");
        if (!value.isAbsolute() || !"https".equalsIgnoreCase(value.getScheme()) || value.getHost() == null
            || value.getUserInfo() != null) {
            throw new IllegalArgumentException(label + " must be an absolute HTTPS URI without user info");
        }
        return value.normalize();
    }

    private static String validateOrigin(URI uri) {
        URI value = validateDocumentUri(uri, "OpenAPI allowed origin");
        if ((!value.getPath().isEmpty() && !"/".equals(value.getPath())) || value.getQuery() != null
                || value.getFragment() != null) {
            throw new IllegalArgumentException("OpenAPI allowed origins must contain only scheme, host, and optional port");
        }
        return origin(value);
    }

    private static String origin(URI uri) {
        int port = uri.getPort() < 0 ? 443 : uri.getPort();
        return "https://" + uri.getHost().toLowerCase(java.util.Locale.ROOT) + ":" + port;
    }

    private static URI withoutFragment(URI uri) {
        if (uri.getFragment() == null) return uri;
        return URI.create(uri.toASCIIString().substring(0, uri.toASCIIString().indexOf('#')));
    }

    private static String extension(URI uri) {
        String path = uri.getPath().toLowerCase(java.util.Locale.ROOT);
        if (path.endsWith(".json")) return ".json";
        if (path.endsWith(".yml")) return ".yml";
        return ".yaml";
    }

    private static void atomicWrite(Path target, byte[] bytes) throws IOException {
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try {
            Files.write(temporary, bytes);
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static String digest(Map<URI, Document> documents) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            documents.entrySet().stream().sorted(Map.Entry.comparingByKey(Comparator.comparing(URI::toASCIIString)))
                .forEach(entry -> {
                    digest.update(entry.getKey().toASCIIString().getBytes(StandardCharsets.UTF_8));
                    digest.update((byte) 0);
                    digest.update(entry.getValue().bytes());
                    digest.update((byte) 0);
                });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    @FunctionalInterface
    interface Fetcher {
        byte[] fetch(URI uri) throws IOException, InterruptedException;
    }

    record Acquisition(String source, String rootSha256, String originalClosureSha256, List<AcquiredDocument> documents) {
        Acquisition {
            source = Objects.requireNonNull(source, "OpenAPI acquisition source must not be null");
            rootSha256 = Objects.requireNonNull(rootSha256, "OpenAPI acquisition root digest must not be null");
            originalClosureSha256 = Objects.requireNonNull(originalClosureSha256,
                "OpenAPI acquisition closure digest must not be null");
            documents = List.copyOf(Objects.requireNonNull(documents, "OpenAPI acquired documents must not be null"));
        }
    }

    record AcquiredDocument(String source, String sha256, String localPath) {
    }

    private record Document(byte[] bytes, JsonNode node) {
        private Document {
            bytes = bytes.clone();
            node = Objects.requireNonNull(node, "OpenAPI document node must not be null");
        }
        @Override public byte[] bytes() { return bytes.clone(); }
    }
}
