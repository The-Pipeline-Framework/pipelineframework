package org.pipelineframework.examples.rag.indexer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.connector.vector.VectorUpsertRequest;

@ApplicationScoped
public final class VectorUpsertCommandIdGenerator implements CommandIdGenerator<VectorUpsertRequest> {
    @Override public String commandId(CommandDescriptor descriptor, VectorUpsertRequest input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, descriptor.command());
            update(digest, input.itemId());
            update(digest, input.content());
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(input.values().size()).array());
            input.values().forEach(value -> digest.update(ByteBuffer.allocate(Integer.BYTES)
                .putInt(Float.floatToRawIntBits(value)).array()));
            return "vector-upsert:" + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
