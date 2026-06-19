package org.pipelineframework.search.common.mapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.objectingest.ObjectSnapshotMapper;
import org.pipelineframework.search.common.domain.RawDocument;
import org.pipelineframework.search.common.util.HashingUtils;

/**
 * Projects a text object snapshot into the Search pipeline raw document input.
 */
public final class SearchObjectDocumentMapper implements ObjectSnapshotMapper<RawDocument> {

    @Override
    public RawDocument map(ObjectSnapshot snapshot) {
        String container = requireText(snapshot.container(), "container");
        String key = requireText(snapshot.key(), "key");
        if (snapshot.textContent() == null || snapshot.textContent().isBlank()) {
            throw new IllegalArgumentException("Search object snapshot must include textContent");
        }
        RawDocument document = new RawDocument();
        document.docId = UUID.nameUUIDFromBytes((container + "/" + key).getBytes(StandardCharsets.UTF_8));
        document.sourceUrl = "s3://" + container + "/" + key;
        document.fetchOptions = "object-ingest";
        document.rawContent = snapshot.textContent();
        document.rawContentHash = HashingUtils.sha256Base64Url(snapshot.textContent());
        document.fetchedAt = java.time.Instant.ofEpochMilli(snapshot.lastModifiedEpochMs());
        return document;
    }

    private String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Search object snapshot must include " + field);
        }
        return value.trim();
    }
}
