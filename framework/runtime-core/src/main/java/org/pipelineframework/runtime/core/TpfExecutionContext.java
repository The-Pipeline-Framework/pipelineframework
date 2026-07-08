/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.runtime.core;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Framework-neutral accessors for request-scoped TPF control/cache/replay context.
 */
public final class TpfExecutionContext {
    private static final String VERSION_TAG_KEY = TpfExecutionContext.class.getName() + ".versionTag";
    private static final String REPLAY_MODE_KEY = TpfExecutionContext.class.getName() + ".replayMode";
    private static final String CACHE_POLICY_KEY = TpfExecutionContext.class.getName() + ".cachePolicy";

    private TpfExecutionContext() {
    }

    /**
     * Returns the current pipeline version tag, when present.
     *
     * @return current version tag
     */
    public static Optional<String> versionTag() {
        return value(VERSION_TAG_KEY);
    }

    /**
     * Returns the current pipeline replay mode, when present.
     *
     * @return current replay mode
     */
    public static Optional<String> replayMode() {
        return value(REPLAY_MODE_KEY);
    }

    /**
     * Returns the current pipeline cache policy, when present.
     *
     * @return current cache policy
     */
    public static Optional<String> cachePolicy() {
        return value(CACHE_POLICY_KEY);
    }

    /**
     * Captures the current context values.
     *
     * @return immutable context snapshot
     */
    public static Snapshot capture() {
        return new Snapshot(versionTag(), replayMode(), cachePolicy());
    }

    /**
     * Sets normalized context values from incoming headers and returns a scope that restores the previous values.
     *
     * @param versionTag incoming version tag
     * @param replayMode incoming replay mode
     * @param cachePolicy incoming cache policy
     * @return closeable scope
     */
    public static Scope withHeaders(String versionTag, String replayMode, String cachePolicy) {
        Snapshot previous = capture();
        put(VERSION_TAG_KEY, normalize(versionTag));
        put(REPLAY_MODE_KEY, normalize(replayMode));
        put(CACHE_POLICY_KEY, normalize(cachePolicy));
        return new Scope(previous);
    }

    /**
     * Restores a captured context snapshot.
     *
     * @param snapshot context snapshot
     */
    public static void restore(Snapshot snapshot) {
        Snapshot actual = Objects.requireNonNull(snapshot, "snapshot");
        put(VERSION_TAG_KEY, actual.versionTag());
        put(REPLAY_MODE_KEY, actual.replayMode());
        put(CACHE_POLICY_KEY, actual.cachePolicy());
    }

    private static Optional<String> value(String key) {
        return normalize(RuntimeAdapters.executionContext(key, String.class));
    }

    private static void put(String key, Optional<String> value) {
        Optional<String> actual = Objects.requireNonNull(value, "value");
        if (actual.isPresent()) {
            RuntimeAdapters.setExecutionContext(key, actual.get());
        } else {
            RuntimeAdapters.clearExecutionContext(key);
        }
    }

    private static Optional<String> normalize(String value) {
        return Optional.ofNullable(value)
            .map(String::trim)
            .filter(actual -> !actual.isEmpty());
    }

    private static Optional<String> normalize(Optional<String> value) {
        return Objects.requireNonNull(value, "value")
            .map(String::trim)
            .filter(actual -> !actual.isEmpty());
    }

    /**
     * Immutable TPF execution context snapshot.
     *
     * @param versionTag captured version tag
     * @param replayMode captured replay mode
     * @param cachePolicy captured cache policy
     */
    public record Snapshot(Optional<String> versionTag, Optional<String> replayMode, Optional<String> cachePolicy) {
        public Snapshot {
            versionTag = normalize(versionTag);
            replayMode = normalize(replayMode);
            cachePolicy = normalize(cachePolicy);
        }
    }

    /**
     * Scope that restores previous TPF execution context values.
     */
    public static final class Scope implements AutoCloseable {
        private final Snapshot previous;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Scope(Snapshot previous) {
            this.previous = Objects.requireNonNull(previous, "previous");
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                restore(previous);
            }
        }
    }
}
