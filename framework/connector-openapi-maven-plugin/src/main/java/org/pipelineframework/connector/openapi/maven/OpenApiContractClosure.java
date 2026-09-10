package org.pipelineframework.connector.openapi.maven;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/** Offline, bounded resolution of the local OpenAPI document closure. */
final class OpenApiContractClosure {
    private static final int MAX_DOCUMENTS = 64;
    private static final long MAX_BYTES = 16L * 1024 * 1024;
    private static final int MAX_REFERENCE_DEPTH = 32;
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();

    private OpenApiContractClosure() {
    }

    static Resolved load(Path snapshot) throws IOException {
        Path root = Objects.requireNonNull(snapshot, "OpenAPI snapshot must not be null").toAbsolutePath().normalize();
        if (!Files.isRegularFile(root)) throw new IllegalArgumentException("OpenAPI snapshot does not exist: " + root);
        Map<Path, byte[]> documents = new LinkedHashMap<>();
        collect(root.getParent(), root, 0, documents, new java.util.LinkedHashSet<>());
        long bytes = documents.values().stream().mapToLong(value -> value.length).sum();
        if (bytes > MAX_BYTES) throw new IllegalArgumentException("OpenAPI reference closure exceeds byte limit");
        JsonNode rootNode = YAML.readTree(documents.get(root));
        String version = requiredText(rootNode, "openapi");
        if (!(version.matches("3\\.0\\.[0-4]") || version.matches("3\\.1\\.[0-2]"))) {
            throw new IllegalArgumentException("unsupported OpenAPI version " + version
                + "; supported versions are 3.0.0-3.0.4 and 3.1.0-3.1.2");
        }
        AcquisitionEvidence acquisition = acquisition(root);
        return new Resolved(root, version, sha256(documents.get(root)), digest(root.getParent(), documents),
            acquisition.rootSha256(), acquisition.closureSha256(), List.copyOf(documents.keySet()), rootNode);
    }

    private static void collect(
        Path contractRoot,
        Path document,
        int depth,
        Map<Path, byte[]> documents,
        Set<Path> active
    )
        throws IOException {
        if (depth > MAX_REFERENCE_DEPTH) throw new IllegalArgumentException("OpenAPI reference depth exceeds limit");
        Path checked = document.toAbsolutePath().normalize();
        if (!checked.startsWith(contractRoot)) {
            throw new IllegalArgumentException("OpenAPI reference escapes the configured contract root: " + document);
        }
        if (active.contains(checked)) {
            throw new IllegalArgumentException("OpenAPI external reference closure contains a cycle at " + checked);
        }
        if (documents.containsKey(checked)) return;
        if (documents.size() == MAX_DOCUMENTS) {
            throw new IllegalArgumentException("OpenAPI reference closure exceeds document limit");
        }
        byte[] bytes = Files.readAllBytes(checked);
        if (bytes.length > MAX_BYTES) throw new IllegalArgumentException("OpenAPI document exceeds byte limit: " + checked);
        documents.put(checked, bytes);
        active.add(checked);
        JsonNode node = YAML.readTree(bytes);
        List<String> references = new ArrayList<>();
        collectReferences(node, references);
        for (String reference : references.stream().sorted().toList()) {
            String resource = reference.split("#", 2)[0];
            if (resource.isBlank()) continue;
            URI uri = URI.create(resource);
            if (uri.isAbsolute() || uri.getAuthority() != null || resource.contains("://")) {
                throw new IllegalArgumentException(
                    "offline OpenAPI import rejects external URI reference; acquire and rewrite it first: " + reference);
            }
            collect(contractRoot, checked.getParent().resolve(resource), depth + 1, documents, active);
        }
        active.remove(checked);
    }

    private static void collectReferences(JsonNode node, List<String> references) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                if ("$ref".equals(entry.getKey()) && entry.getValue().isTextual()) {
                    references.add(entry.getValue().textValue());
                } else {
                    collectReferences(entry.getValue(), references);
                }
            });
        } else if (node.isArray()) {
            node.forEach(value -> collectReferences(value, references));
        }
    }

    private static String digest(Path root, Map<Path, byte[]> documents) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            documents.entrySet().stream().sorted(Comparator.comparing(entry -> root.relativize(entry.getKey()).toString()))
                .forEach(entry -> {
                    digest.update(root.relativize(entry.getKey()).toString().replace('\\', '/').getBytes(StandardCharsets.UTF_8));
                    digest.update((byte) 0);
                    digest.update(entry.getValue());
                    digest.update((byte) 0);
                });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String sha256(byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String requiredText(JsonNode node, String field) {
        String value = node.path(field).asText("").trim();
        if (value.isEmpty()) throw new IllegalArgumentException("OpenAPI document requires '" + field + "'");
        return value;
    }

    private static AcquisitionEvidence acquisition(Path root) throws IOException {
        Path manifest = OpenApiAcquisition.manifest(root);
        if (!Files.isRegularFile(manifest)) return new AcquisitionEvidence("", "");
        JsonNode node = JSON.readTree(Files.readAllBytes(manifest));
        String rootDigest = digest(node, "rootSha256");
        String closureDigest = digest(node, "originalClosureSha256");
        JsonNode documents = node.path("documents");
        if (!documents.isArray() || documents.isEmpty() || documents.size() > MAX_DOCUMENTS) {
            throw new IllegalArgumentException("OpenAPI acquisition manifest has an invalid document list");
        }
        for (JsonNode document : documents) {
            digest(document, "sha256");
            String localPath = requiredText(document, "localPath");
            Path local = root.getParent().resolve(localPath).normalize();
            if (!local.startsWith(root.getParent()) || !Files.isRegularFile(local)) {
                throw new IllegalArgumentException("OpenAPI acquisition manifest references a missing local document");
            }
        }
        return new AcquisitionEvidence(rootDigest, closureDigest);
    }

    private static String digest(JsonNode node, String field) {
        String value = requiredText(node, field).toLowerCase(java.util.Locale.ROOT);
        if (!value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("OpenAPI acquisition manifest " + field + " must be SHA-256");
        }
        return value;
    }

    record Resolved(
        Path root,
        String version,
        String rootDigest,
        String digest,
        String acquiredRootDigest,
        String acquiredClosureDigest,
        List<Path> documents,
        JsonNode rootNode
    ) {
    }

    private record AcquisitionEvidence(String rootSha256, String closureSha256) {
    }
}
