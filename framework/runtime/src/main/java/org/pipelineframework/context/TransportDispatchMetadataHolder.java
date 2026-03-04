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

package org.pipelineframework.context;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Holds transport dispatch metadata using Vert.x context when available, and falls back to thread-local storage.
 */
public final class TransportDispatchMetadataHolder {

    private static final String CONTEXT_KEY = TransportDispatchMetadataHolder.class.getName() + ".context";
    private static final ThreadLocal<TransportDispatchMetadata> THREAD_LOCAL = new ThreadLocal<>();

    /**
     * Prevents instantiation of this utility class.
     */
    private TransportDispatchMetadataHolder() {
    }

    /**
     * Obtain the current transport dispatch metadata, preferring the Vert.x Context local storage when available.
     *
     * If the active Vert.x Context does not support locals or is absent, falls back to a thread-local value.
     *
     * @return the current TransportDispatchMetadata, or {@code null} if none is set
     */
    public static TransportDispatchMetadata get() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                Object stored = context.getLocal(CONTEXT_KEY);
                if (stored instanceof TransportDispatchMetadata metadata) {
                    return metadata;
                }
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        return THREAD_LOCAL.get();
    }

    /**
     * Sets current transport dispatch metadata.
     *
     * @param metadata metadata to store
     */
    public static void set(TransportDispatchMetadata metadata) {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                context.putLocal(CONTEXT_KEY, metadata);
                THREAD_LOCAL.remove();
                return;
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        THREAD_LOCAL.set(metadata);
    }

    /**
     * Removes any transport dispatch metadata stored for the current execution context.
     *
     * Attempts to remove the metadata from the current Vert.x Context locals when a context is available,
     * and always clears the ThreadLocal fallback.
     */
    public static void clear() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                context.removeLocal(CONTEXT_KEY);
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        THREAD_LOCAL.remove();
    }
}
