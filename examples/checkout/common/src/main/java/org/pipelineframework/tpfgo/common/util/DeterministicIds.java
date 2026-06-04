package org.pipelineframework.tpfgo.common.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class DeterministicIds {

    private DeterministicIds() {
    }

    public static UUID uuid(String namespace, String... parts) {
        StringBuilder seed = new StringBuilder(namespace == null ? "tpfgo" : namespace);
        if (parts == null) {
            throw new NullPointerException("parts must not be null");
        }
        for (int index = 0; index < parts.length; index++) {
            String part = parts[index];
            if (part == null) {
                throw new NullPointerException("parts[" + index + "] must not be null");
            }
            seed.append('|').append(part);
        }
        return UUID.nameUUIDFromBytes(seed.toString().getBytes(StandardCharsets.UTF_8));
    }
}
