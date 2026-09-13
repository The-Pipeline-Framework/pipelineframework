package org.pipelineframework.objectpublish;

/**
 * Incremental object payload bytes produced by a streaming publish renderer.
 *
 * @param bytes encoded payload bytes for this chunk
 */
public record ObjectPayloadChunk(byte[] bytes) {
    public static final ObjectPayloadChunk EMPTY = new ObjectPayloadChunk(new byte[0]);

    public ObjectPayloadChunk {
        bytes = bytes == null ? new byte[0] : bytes.clone();
    }

    @Override
    public byte[] bytes() {
        return bytes.clone();
    }
}
