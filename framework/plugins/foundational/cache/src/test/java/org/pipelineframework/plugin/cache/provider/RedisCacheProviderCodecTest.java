package org.pipelineframework.plugin.cache.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.ProtobufMessageParser;
import org.pipelineframework.cache.QueryNotFoundCacheEntry;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;

class RedisCacheProviderCodecTest {

    @Test
    void jsonEnvelopeRoundTripSupportsBuilderDtos() {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.objectMapper = new ObjectMapper();

        ExampleDto input = ExampleDto.builder()
            .id("doc-123")
            .count(7)
            .build();

        String serialized = provider.serialize(input).orElseThrow();
        Optional<Object> decoded = provider.deserialize(serialized, "cache-key");

        assertTrue(decoded.isPresent());
        assertEquals(input, decoded.get());
    }

    @Test
    void queryNotFoundMarkerSurvivesTheExistingJsonCacheEnvelope() {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.objectMapper = new ObjectMapper();
        QueryNotFoundCacheEntry input = new QueryNotFoundCacheEntry("customer-missing");

        String serialized = provider.serialize(input).orElseThrow();
        Optional<Object> decoded = provider.deserialize(serialized, "query-key");

        assertEquals(Optional.of(input), decoded);
    }

    @Test
    void protobufEnvelopeRoundTripUsesParserRegistry() throws Exception {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.objectMapper = new ObjectMapper();
        setParserRegistry(provider, StringValue.class.getName(), new StringValueParser());

        StringValue input = StringValue.of("hello");
        String serialized = provider.serialize(input).orElseThrow();
        Optional<Object> decoded = provider.deserialize(serialized, "proto-key");

        assertTrue(decoded.isPresent());
        assertEquals(input, decoded.get());
    }

    @Test
    void protobufEnvelopeWithoutParserReturnsEmpty() throws Exception {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.objectMapper = new ObjectMapper();
        setParserRegistry(provider, "missing.Parser", new ProtobufMessageParser() {
            @Override
            public String type() {
                return "missing.Parser";
            }

            @Override
            public com.google.protobuf.Message parseFrom(byte[] bytes) {
                return StringValue.of("ignored");
            }
        });

        StringValue input = StringValue.of("hello");
        String serialized = provider.serialize(input).orElseThrow();
        Optional<Object> decoded = provider.deserialize(serialized, "proto-key");

        assertFalse(decoded.isPresent());
    }

    @Test
    void unknownEncodingReturnsEmpty() throws Exception {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.objectMapper = new ObjectMapper();

        String payload = Base64.getEncoder().encodeToString("data".getBytes());
        String serialized = provider.objectMapper.writeValueAsString(
            new RedisCacheProvider.CacheEnvelope(StringValue.class.getName(), payload, "unknown"));

        Optional<Object> decoded = provider.deserialize(serialized, "proto-key");

        assertFalse(decoded.isPresent());
    }

    @Test
    void skipsRedisWriteWhenSerializationFails() throws Exception {
        RedisCacheProvider provider = new RedisCacheProvider();
        provider.keyPrefix = "cache:";
        provider.redis = mock(ReactiveRedisDataSource.class);
        ObjectMapper mapper = mock(ObjectMapper.class);
        provider.objectMapper = mapper;
        when(mapper.writeValueAsString(org.mockito.ArgumentMatchers.any()))
            .thenThrow(new JsonProcessingException("cannot serialize") { });

        Object value = new Object();
        assertEquals(value, provider.cache("key", value).await().indefinitely());

        verifyNoInteractions(provider.redis);
    }

    private void setParserRegistry(
        RedisCacheProvider provider,
        String type,
        ProtobufMessageParser parser) throws Exception {
        Field field = RedisCacheProvider.class.getDeclaredField("protobufParserByType");
        field.setAccessible(true);
        field.set(provider, java.util.Map.of(type, parser));
    }

    static final class StringValueParser implements ProtobufMessageParser {
        @Override
        public String type() {
            return StringValue.class.getName();
        }

        @Override
        public com.google.protobuf.Message parseFrom(byte[] bytes) {
            try {
                return StringValue.parseFrom(bytes);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse StringValue", e);
            }
        }
    }

    @JsonDeserialize(builder = ExampleDto.Builder.class)
    static final class ExampleDto {
        private final String id;
        private final int count;

        private ExampleDto(Builder builder) {
            this.id = builder.id;
            this.count = builder.count;
        }

        static Builder builder() {
            return new Builder();
        }

        public String getId() {
            return id;
        }

        public int getCount() {
            return count;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            ExampleDto that = (ExampleDto) other;
            if (count != that.count) {
                return false;
            }
            return id != null ? id.equals(that.id) : that.id == null;
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + count;
            return result;
        }

        @JsonPOJOBuilder(withPrefix = "")
        static final class Builder {
            private String id;
            private int count;

            public Builder id(String id) {
                this.id = id;
                return this;
            }

            public Builder count(int count) {
                this.count = count;
                return this;
            }

            public ExampleDto build() {
                return new ExampleDto(this);
            }
        }
    }
}
