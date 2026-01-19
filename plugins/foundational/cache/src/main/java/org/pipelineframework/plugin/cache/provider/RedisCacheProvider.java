/*
 * Copyright (c) 2023-2025 Mariano Barcia
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

package org.pipelineframework.plugin.cache.provider;

import java.time.Duration;
import java.util.Optional;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;

import io.quarkus.arc.Unremovable;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.keys.ReactiveKeyCommands;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.cache.CacheProvider;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Redis-based cache provider using the Quarkus Redis client.
 */
@ApplicationScoped
@Unremovable
@IfBuildProperty(name = "pipeline.cache.provider", stringValue = "redis")
@ParallelismHint(ordering = OrderingRequirement.RELAXED, threadSafety = ThreadSafety.SAFE)
public class RedisCacheProvider implements CacheProvider<Object> {

    private static final Logger LOG = Logger.getLogger(RedisCacheProvider.class);

    @ConfigProperty(name = "pipeline.cache.redis.prefix", defaultValue = "pipeline-cache:")
    String keyPrefix;

    @Inject
    ReactiveRedisDataSource redis;

    private final Jsonb jsonb = JsonbBuilder.create();

    /**
     * Default constructor for RedisCacheProvider.
     */
    public RedisCacheProvider() {
    }

    @Override
    public Class<Object> type() {
        return Object.class;
    }

    @Override
    public Uni<Object> cache(String key, Object value) {
        return cache(key, value, null);
    }

    @Override
    public Uni<Object> cache(String key, Object value, Duration ttl) {
        if (value == null) {
            return Uni.createFrom().nullItem();
        }
        if (key == null || key.isBlank()) {
            LOG.warn("Cache key is null or blank, skipping cache");
            return Uni.createFrom().item(value);
        }

        String fullKey = keyPrefix + key;
        ReactiveValueCommands<String, String> values = redis.value(String.class);
        String serialized = jsonb.toJson(new CacheEnvelope(value.getClass().getName(), jsonb.toJson(value)));

        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return values.set(fullKey, serialized).replaceWith(value);
        } else {
            return values.setex(fullKey, ttl.getSeconds(), serialized).replaceWith(value);
        }
    }

    @Override
    public Uni<Optional<Object>> get(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(Optional.empty());
        }
        ReactiveValueCommands<String, String> values = redis.value(String.class);
        return values.get(keyPrefix + key)
            .onItem().transform(serialized -> deserialize(serialized, key));
    }

    @Override
    public Uni<Boolean> exists(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(false);
        }
        ReactiveKeyCommands<String> keys = redis.key();
        return keys.exists(keyPrefix + key);
    }

    @Override
    public Uni<Boolean> invalidate(String key) {
        if (key == null || key.isBlank()) {
            return Uni.createFrom().item(false);
        }
        ReactiveKeyCommands<String> keys = redis.key();
        return keys.del(keyPrefix + key).map(count -> count != null && count > 0);
    }

    @Override
    public Uni<Boolean> invalidateByPrefix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return Uni.createFrom().item(false);
        }
        ReactiveKeyCommands<String> keys = redis.key();
        String pattern = keyPrefix + prefix + "*";
        return keys.keys(pattern)
            .onItem().transformToUni(found -> {
                if (found == null || found.isEmpty()) {
                    return Uni.createFrom().item(false);
                }
                String[] keyArray = found.toArray(new String[0]);
                return keys.del(keyArray).map(count -> count != null && count > 0);
            });
    }

    @Override
    public String backend() {
        return "redis";
    }

    @Override
    public boolean supports(Object item) {
        return true;
    }

    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }

    private Optional<Object> deserialize(String serialized, String key) {
        if (serialized == null || serialized.isBlank()) {
            return Optional.empty();
        }
        try {
            CacheEnvelope envelope = jsonb.fromJson(serialized, CacheEnvelope.class);
            Class<?> clazz = Class.forName(envelope.type());
            Object value = jsonb.fromJson(envelope.payload(), clazz);
            return Optional.ofNullable(value);
        } catch (Exception e) {
            LOG.warnf("Failed to deserialize cache entry for key %s: %s", key, e.getMessage());
            return Optional.empty();
        }
    }

    public static record CacheEnvelope(String type, String payload) {
    }
}
