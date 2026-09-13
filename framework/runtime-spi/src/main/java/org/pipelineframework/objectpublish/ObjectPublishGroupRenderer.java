package org.pipelineframework.objectpublish;

import java.util.Map;

/**
 * Streaming renderer for one Object Publish group.
 *
 * @param <T> terminal pipeline output item type
 */
public interface ObjectPublishGroupRenderer<T> {
    String contentType();

    default Map<String, String> initialLabels() {
        return Map.of();
    }

    default ObjectPayloadChunk onOpen() {
        return ObjectPayloadChunk.EMPTY;
    }

    ObjectPayloadChunk onItem(T item);

    default ObjectPayloadChunk onClose() {
        return ObjectPayloadChunk.EMPTY;
    }

    default Map<String, String> finalMetadata() {
        return Map.of();
    }
}
