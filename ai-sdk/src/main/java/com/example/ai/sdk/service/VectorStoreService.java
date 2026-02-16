package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.StoreResultDto;
import com.example.ai.sdk.entity.StoreResult;
import com.example.ai.sdk.entity.Vector;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vector store service that stores vector embeddings.
 * Implements Uni -> Uni cardinality (UnaryUnary).
 */
public class VectorStoreService implements ReactiveService<Vector, StoreResult> {

    private final Map<String, Vector> store = new ConcurrentHashMap<>();

    @Override
    public Uni<StoreResult> process(Vector input) {
        if (input == null || input.id() == null) {
            return Uni.createFrom().failure(
                new IllegalArgumentException("input and input.id() must be non-null")
            );
        }

        // Store the vector
        store.put(input.id(), input);

        StoreResult result = new StoreResult(
            input.id(),
            true,
            "Vector stored successfully"
        );

        return Uni.createFrom().item(result);
    }

    /**
     * Alternative method that accepts DTOs directly for TPF delegation
     */
    public Uni<StoreResultDto> processDto(Vector input) {
        return process(input).map(r -> new StoreResultDto(r.id(), r.success(), r.message()));
    }

    /**
     * Retrieve a vector by ID
     */
    public Vector getVector(String id) {
        return store.get(id);
    }
}
