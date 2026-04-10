package org.pipelineframework.tpfgo.common.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class DeterministicIds {

    private DeterministicIds() {
    }

    public static UUID uuid(String namespace, String... parts) {
        StringBuilder seed = new StringBuilder(namespace == null ? "tpfgo" : namespace);
        if (parts != null) {
            for (String part : parts) {
                seed.append('|').append(part == null ? "" : part);
            }
        }
        return UUID.nameUUIDFromBytes(seed.toString().getBytes(StandardCharsets.UTF_8));
    }
}
