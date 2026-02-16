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

    /**
     * Persist the given vector in the in-memory store and produce a StoreResult describing the outcome.
     *
     * @param input the vector to store; must be non-null and must have a non-null id()
     * @return a StoreResult containing the stored vector's id, `success` set to `true`, and a success message
     */
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
     * Process a Vector and convert the operation result into a transfer object suitable for TPF delegation.
     *
     * @param input the Vector to persist; must not be null and must have a non-null id()
     * @return a StoreResultDto containing the vector id, a success flag, and a message describing the outcome
     */
    public Uni<StoreResultDto> processDto(Vector input) {
        return process(input).map(r -> new StoreResultDto(r.id(), r.success(), r.message()));
    }

    /**
     * Retrieve the vector stored under the given identifier.
     *
     * @param id the vector's identifier
     * @return the stored {@code Vector} for the given id, or {@code null} if no vector is found
     */
    public Vector getVector(String id) {
        return store.get(id);
    }
}