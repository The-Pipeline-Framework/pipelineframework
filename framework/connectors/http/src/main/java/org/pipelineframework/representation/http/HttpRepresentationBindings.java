package org.pipelineframework.representation.http;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.connector.http.HttpOperationBindingCatalog;
import org.pipelineframework.connector.http.HttpOperationRepresentationBinding;
import org.pipelineframework.connector.http.HttpRepresentationMode;

/** Runtime materialization of compiler-pinned mapper identities. */
public final class HttpRepresentationBindings {
    private static final ObjectMapper JSON = PipelineJson.mapper();

    private final HttpOperationBindingCatalog catalogue;
    private final ClassLoader classLoader;
    private final Map<String, Mapper<Object, Object>> mappers = new ConcurrentHashMap<>();

    public HttpRepresentationBindings(HttpOperationBindingCatalog catalogue, ClassLoader classLoader) {
        this.catalogue = Objects.requireNonNull(catalogue, "HTTP representation catalogue must not be null");
        this.classLoader = Objects.requireNonNull(classLoader, "HTTP representation classloader must not be null");
    }

    public JsonNode toWire(String mappingKey, Object input) {
        HttpOperationRepresentationBinding binding = require(mappingKey);
        Object wire = binding.mode() == HttpRepresentationMode.DIRECT ? input : mapper(binding).toExternal(input);
        if (wire == null) throw new IllegalStateException("HTTP request mapper returned null");
        return JSON.valueToTree(wire);
    }

    public Object fromWire(String mappingKey, JsonNode wire, Class<?> outputType) {
        HttpOperationRepresentationBinding binding = require(mappingKey);
        try {
            if (binding.mode() == HttpRepresentationMode.DIRECT) {
                return requireOutput(JSON.treeToValue(wire, outputType), outputType);
            }
            Class<?> representationType = Class.forName(binding.representationType().orElseThrow(), true, classLoader);
            Object external = JSON.treeToValue(wire, representationType);
            Object output = mapper(binding).fromExternal(external);
            return requireOutput(output, outputType);
        } catch (ReflectiveOperationException | com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("unable to materialize pinned HTTP representation", failure);
        }
    }

    private Object requireOutput(Object output, Class<?> outputType) {
        if (output == null || !org.apache.commons.lang3.ClassUtils.primitiveToWrapper(outputType).isInstance(output)) {
            throw new IllegalStateException("HTTP response mapper returned an incompatible value");
        }
        return output;
    }

    private HttpOperationRepresentationBinding require(String mappingKey) {
        return catalogue.find(mappingKey).orElseThrow(() ->
            new IllegalStateException("missing compiler-resolved HTTP representation mapping: " + mappingKey));
    }

    @SuppressWarnings("unchecked")
    private Mapper<Object, Object> mapper(HttpOperationRepresentationBinding binding) {
        return mappers.computeIfAbsent(binding.mappingKey(), ignored -> {
            try {
                Class<?> type = Class.forName(binding.mapperType().orElseThrow(), true, classLoader);
                if (!Mapper.class.isAssignableFrom(type)) {
                    throw new IllegalStateException("pinned HTTP mapper does not implement Mapper: " + type.getName());
                }
                return (Mapper<Object, Object>) type.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException failure) {
                throw new IllegalStateException("unable to construct pinned HTTP mapper", failure);
            }
        });
    }
}
