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

package org.pipelineframework.awaitable;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * In-memory bridge from durable await completion admission to a live brokered await stream.
 */
@ApplicationScoped
public class AwaitLiveCompletionRegistry {

    private final ConcurrentMap<Key, LiveAwaitSession<?>> sessions = new ConcurrentHashMap<>();

    <O> LiveAwaitSession<O> open(AwaitStepDescriptor descriptor, String tenantId, String unitId) {
        Objects.requireNonNull(descriptor, "descriptor must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(unitId, "unitId must not be null");
        Class<?> outputType = resolveOutputType(descriptor);
        Key key = new Key(tenantId, unitId);
        AtomicReference<LiveAwaitSession<?>> sessionRef = new AtomicReference<>();
        LiveAwaitSession<O> session = new LiveAwaitSession<>(
            unitId,
            outputType,
            () -> sessions.remove(key, sessionRef.get()));
        sessionRef.set(session);
        LiveAwaitSession<?> existing = sessions.putIfAbsent(key, session);
        if (existing != null) {
            throw new IllegalStateException("A live await stream already owns await unit " + unitId);
        }
        return session;
    }

    public Uni<Boolean> signal(AwaitInteractionRecord record, AwaitUnitRecord unit) {
        if (record == null || unit == null) {
            return Uni.createFrom().item(false);
        }
        LiveAwaitSession<?> session = sessions.get(new Key(record.tenantId(), record.unitId()));
        if (session == null) {
            return Uni.createFrom().item(false);
        }
        return session.accept(record).replaceWith(Boolean.TRUE);
    }

    void close(String tenantId, String unitId, LiveAwaitSession<?> expectedSession) {
        if (tenantId != null && unitId != null && expectedSession != null) {
            sessions.remove(new Key(tenantId, unitId), expectedSession);
        }
    }

    private static Class<?> resolveOutputType(AwaitStepDescriptor descriptor) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = AwaitPayloadSupport.class.getClassLoader();
            }
            return AwaitPayloadSupport.resolvePayloadClass(descriptor.outputType(), loader);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "Failed resolving await output type " + descriptor.outputType()
                    + " for live await stream " + descriptor.stepId(),
                e);
        }
    }

    private record Key(String tenantId, String unitId) {
    }

}
