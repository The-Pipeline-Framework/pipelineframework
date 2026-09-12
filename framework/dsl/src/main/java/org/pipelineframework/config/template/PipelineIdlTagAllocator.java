/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.pipelineframework.config.template;

import java.util.HashSet;
import java.util.Set;

/** Allocates protobuf tags without using the reserved implementation range. */
final class PipelineIdlTagAllocator {

    private static final int RESERVED_START = 19_000;
    private static final int RESERVED_END = 19_999;

    int allocate(Set<Integer> unavailable) {
        Set<Integer> used = new HashSet<>(unavailable);
        for (int candidate = 1; candidate < Integer.MAX_VALUE; candidate++) {
            if (candidate >= RESERVED_START && candidate <= RESERVED_END) {
                candidate = RESERVED_END;
                continue;
            }
            if (!used.contains(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("No legal protobuf field numbers remain");
    }

    boolean isLegal(int number) {
        return number > 0 && (number < RESERVED_START || number > RESERVED_END);
    }
}
