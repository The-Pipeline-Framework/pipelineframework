package org.pipelineframework.examples.rag.indexer;

import java.util.Objects;
import org.pipelineframework.examples.rag.indexer.domain.Document;
import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.objectingest.ObjectSnapshotMapper;

/** Projects immutable object provenance into the public INDEXER input. */
public final class DocumentObjectMapper implements ObjectSnapshotMapper<Document> {
    @Override
    public Document map(ObjectSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "object snapshot must not be null");
        var reference = Objects.requireNonNull(snapshot.contentRef(), "object snapshot must carry a payload reference");
        String revision = reference.version() != null ? reference.version() : reference.checksum();
        if (revision == null) revision = snapshot.etag();
        if (revision == null) throw new IllegalArgumentException("document object must have immutable version provenance");
        String container = reference.container() == null ? "" : reference.container() + "/";
        return new Document(reference.provider() + ":" + container + reference.key() + "@" + revision, reference);
    }
}
