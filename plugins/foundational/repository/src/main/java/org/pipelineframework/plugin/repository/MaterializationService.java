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

package org.pipelineframework.plugin.repository;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.RepositoryChecksums;
import org.pipelineframework.repository.RepositoryReadResult;
import org.pipelineframework.repository.RepositoryWriteRequest;

@ApplicationScoped
@Unremovable
public class MaterializationService {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final ObjectMapper MAPPER = PipelineJson.mapper().copy().findAndRegisterModules();

    @Inject
    RepositoryManager repositoryManager;

    public <T> Uni<T> referenceFields(
        T item,
        Class<T> itemType,
        Map<String, String> fieldToRefField,
        long minBytes,
        String container,
        String version
    ) {
        if (item == null || fieldToRefField == null || fieldToRefField.isEmpty()) {
            return Uni.createFrom().item(item);
        }
        Map<String, Object> carrier = toMap(item);
        Uni<Map<String, Object>> chain = Uni.createFrom().item(carrier);
        for (Map.Entry<String, String> entry : fieldToRefField.entrySet()) {
            chain = chain.onItem().transformToUni(current ->
                referenceField(item, current, entry.getKey(), entry.getValue(), minBytes, container, version));
        }
        return chain.map(updated -> MAPPER.convertValue(updated, itemType));
    }

    public <T> Uni<T> dereferenceFields(T item, Class<T> itemType, Map<String, String> fieldToRefField) {
        if (item == null || fieldToRefField == null || fieldToRefField.isEmpty()) {
            return Uni.createFrom().item(item);
        }
        Map<String, Object> carrier = toMap(item);
        Uni<Map<String, Object>> chain = Uni.createFrom().item(carrier);
        for (Map.Entry<String, String> entry : fieldToRefField.entrySet()) {
            chain = chain.onItem().transformToUni(current ->
                dereferenceField(current, entry.getKey(), entry.getValue()));
        }
        return chain.map(updated -> MAPPER.convertValue(updated, itemType));
    }

    private Uni<Map<String, Object>> referenceField(
        Object source,
        Map<String, Object> carrier,
        String fieldName,
        String refFieldName,
        long minBytes,
        String container,
        String version
    ) {
        Object value = readProperty(source, fieldName);
        if (value == null || carrier.get(refFieldName) != null) {
            return Uni.createFrom().item(carrier);
        }
        EncodedField encoded = encode(value, fieldName);
        if (encoded.payload().length < Math.max(0, minBytes)) {
            return Uni.createFrom().item(carrier);
        }
        String checksum = RepositoryChecksums.sha256Hex(encoded.payload());
        String key = checksum.substring(0, 2) + "/" + checksum;
        RepositoryWriteRequest request = new RepositoryWriteRequest(
            container,
            key,
            encoded.payload(),
            encoded.contentType(),
            encoded.codec(),
            checksum,
            version,
            Map.of("field", fieldName));
        return repositoryManager.store(request).map(reference -> {
            Map<String, Object> updated = new LinkedHashMap<>(carrier);
            updated.put(fieldName, null);
            updated.put(refFieldName, reference);
            return updated;
        });
    }

    private Uni<Map<String, Object>> dereferenceField(Map<String, Object> carrier, String fieldName, String refFieldName) {
        if (carrier.get(fieldName) != null || carrier.get(refFieldName) == null) {
            return Uni.createFrom().item(carrier);
        }
        PayloadReference reference = MAPPER.convertValue(carrier.get(refFieldName), PayloadReference.class);
        return repositoryManager.load(reference).map(result -> {
            Map<String, Object> updated = new LinkedHashMap<>(carrier);
            updated.put(fieldName, decode(result));
            return updated;
        });
    }

    private Object decode(RepositoryReadResult result) {
        String codec = result.codec() == null ? "" : result.codec();
        if ("bytes".equals(codec)) {
            return result.payload();
        }
        return new String(result.payload(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private EncodedField encode(Object value, String fieldName) {
        if (value instanceof byte[] bytes) {
            return new EncodedField(bytes.clone(), "application/octet-stream", "bytes");
        }
        if (value instanceof String text) {
            return new EncodedField(text.getBytes(java.nio.charset.StandardCharsets.UTF_8), "text/plain; charset=utf-8", "string");
        }
        throw new IllegalArgumentException("Field '" + fieldName + "' materialization supports only String and byte[] in v1");
    }

    private Object readProperty(Object source, String fieldName) {
        try {
            Method method = source.getClass().getMethod(fieldName);
            return method.invoke(source);
        } catch (NoSuchMethodException ignored) {
            try {
                Method getter = source.getClass().getMethod("get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1));
                return getter.invoke(source);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot read materialized field '" + fieldName + "'", e);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot read materialized field '" + fieldName + "'", e);
        }
    }

    private Map<String, Object> toMap(Object item) {
        return new LinkedHashMap<>(MAPPER.convertValue(item, MAP_TYPE));
    }

    private record EncodedField(byte[] payload, String contentType, String codec) {
    }
}
