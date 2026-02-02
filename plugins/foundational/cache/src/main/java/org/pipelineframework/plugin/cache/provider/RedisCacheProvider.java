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
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import org.pipelineframework.cache.ProtobufMessageParser;

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

    @Inject
    Instance<ProtobufMessageParser> protobufParsers;

    @Inject
    ObjectMapper objectMapper;
    private Map<String, ProtobufMessageParser> protobufParserByType = Map.of();

    /**
     * Default constructor for RedisCacheProvider.
     */
    public RedisCacheProvider() {
    }

    /**
     * Initializes the mapping of protobuf message type names to their parsers after dependency injection.
     *
     * If no parser instances are provided, the map is set empty. Otherwise, null parsers and parsers
     * with null or blank type names are ignored; for duplicate type names the first discovered parser
     * is retained.
     */
    @PostConstruct
    void initParsers() {
        if (protobufParsers == null) {
            protobufParserByType = Map.of();
            return;
        }
        protobufParserByType = protobufParsers.stream()
            .filter(parser -> parser != null && parser.type() != null && !parser.type().isBlank())
            .collect(Collectors.toUnmodifiableMap(
                ProtobufMessageParser::type,
                Function.identity(),
                (existing, ignored) -> existing));
    }

    /**
     * The cache value type handled by this provider.
     *
     * @return the Class object representing the provider's value type (Object.class)
     */
    @Override
    public Class<Object> type() {
        return Object.class;
    }

    @Override
    public Uni<Object> cache(String key, Object value) {
        return cache(key, value, null);
    }

    /**
     * Stores the provided value in Redis under the configured prefix and given key, optionally setting an expiration.
     *
     * @param key   the cache key (without prefix); if null or blank the cache operation is skipped and the original value is returned
     * @param value the value to cache; if null no caching is performed and `null` is returned
     * @param ttl   optional time-to-live for the entry; if null, zero, or negative the entry is stored without expiration
     * @return      the original value passed to the method, or `null` if the provided value was `null`
     */
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
        String serialized = serialize(value);

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

    /**
     * Indicates the thread-safety level of this cache provider.
     *
     * @return the thread-safety level: {@link ThreadSafety#SAFE}
     */
    @Override
    public ThreadSafety threadSafety() {
        return ThreadSafety.SAFE;
    }

    /**
     * Deserialize a stored cache entry string into its original object.
     *
     * <p>Supports entries encoded as JSON or as protobuf payloads wrapped in a CacheEnvelope.
     *
     * @param serialized the JSON representation of a CacheEnvelope containing the value's type, payload, and encoding
     * @param key        the cache key associated with this entry (used for logging context)
     * @return           an Optional containing the deserialized value if successful; `Optional.empty()` if the input is blank,
     *                   the encoding is unsupported, a required protobuf parser is not registered, or deserialization fails
     */
    Optional<Object> deserialize(String serialized, String key) {
        if (serialized == null || serialized.isBlank()) {
            return Optional.empty();
        }
        try {
            CacheEnvelope envelope = objectMapper.readValue(serialized, CacheEnvelope.class);
            Class<?> clazz = Class.forName(envelope.type());
            String encoding = envelope.encoding();
            if (encoding == null || encoding.isBlank() || "json".equalsIgnoreCase(encoding)) {
                Object value = objectMapper.readValue(envelope.payload(), clazz);
                return Optional.ofNullable(value);
            }
            if ("protobuf".equalsIgnoreCase(encoding)) {
                byte[] bytes = Base64.getDecoder().decode(envelope.payload());
                ProtobufMessageParser parser = protobufParserByType.get(envelope.type());
                if (parser == null) {
                    LOG.warnf("No protobuf parser registered for type %s, skipping cache entry for key %s",
                        envelope.type(), key);
                    return Optional.empty();
                }
                return Optional.ofNullable(parser.parseFrom(bytes));
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warnf("Failed to deserialize cache entry for key %s: %s", key, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Create a CacheEnvelope JSON string for the given value, encoding Protobuf messages as Base64.
     *
     * If the value is a Protobuf `Message`, its binary form is Base64-encoded and the envelope's `encoding` is `"protobuf"`.
     * Otherwise the value is serialized to a JSON payload and the envelope's `encoding` is `"json"`.
     *
     * @param value the object to serialize (may be a Protobuf `Message` or any POJO)
     * @return a JSON string representing the CacheEnvelope (contains `type`, `payload`, and `encoding`), or {@code null} if serialization fails
     */
    String serialize(Object value) {
        try {
            if (value instanceof com.google.protobuf.Message message) {
                String payload = Base64.getEncoder().encodeToString(message.toByteArray());
                return objectMapper.writeValueAsString(new CacheEnvelope(value.getClass().getName(), payload, "protobuf"));
            }
            String payload = objectMapper.writeValueAsString(value);
            return objectMapper.writeValueAsString(new CacheEnvelope(value.getClass().getName(), payload, "json"));
        } catch (Exception e) {
            LOG.warnf("Failed to serialize cache entry for type %s: %s", value.getClass().getName(), e.getMessage());
            return null;
        }
    }

    public static record CacheEnvelope(String type, String payload, String encoding) {
    }
}