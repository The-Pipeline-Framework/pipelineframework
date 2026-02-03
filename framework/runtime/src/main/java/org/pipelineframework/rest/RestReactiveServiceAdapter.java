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

package org.pipelineframework.rest;

import java.util.concurrent.CancellationException;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

/**
 * Base adapter for REST resources that process DTO inputs via a ReactiveService and convert outputs to DTOs.
 *
 * @param <DtoIn> The DTO input type
 * @param <DtoOut> The DTO output type
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 */
public abstract class RestReactiveServiceAdapter<DtoIn, DtoOut, DomainIn, DomainOut> {

    private volatile String cachedServiceName;

    /**
     * Default constructor for RestReactiveServiceAdapter.
     */
    public RestReactiveServiceAdapter() {
    }

    /**
     * Supply the ReactiveService that performs processing of domain inputs to domain outputs.
     *
     * Implementations must provide the service instance the adapter will delegate domain processing to.
     *
     * @return the {@code ReactiveService<DomainIn, DomainOut>} instance used to process domain inputs into domain outputs
     */
    protected abstract ReactiveService<DomainIn, DomainOut> getService();

    /**
     * Resolve the service name used for telemetry attributes.
     *
     * @return the service name
     */
    protected String getServiceName() {
        String name = cachedServiceName;
        if (name != null) {
            return name;
        }
        Class<?> serviceClass = getService().getClass();
        name = serviceClass.getSimpleName();
        if (name.contains("_Subclass") && serviceClass.getSuperclass() != null) {
            name = serviceClass.getSuperclass().getSimpleName();
        }
        cachedServiceName = name;
        return name;
    }

    /**
     * Convert a REST DTO input into the corresponding domain object.
     *
     * @param dtoIn the REST input DTO to convert
     * @return the domain input object produced from the DTO
     */
    protected abstract DomainIn fromDto(DtoIn dtoIn);

    /**
     * Converts a processed domain object to its REST DTO representation.
     *
     * @param domainOut the processed domain model instance to convert
     * @return the DTO representation to be returned by the REST resource
     */
    protected abstract DtoOut toDto(DomainOut domainOut);

    /**
     * Process a REST request through the reactive domain service.
     *
     * Converts the REST DTO to a domain input, invokes the domain reactive service, and converts
     * the resulting domain output back to a REST DTO.
     *
     * @param dtoRequest the incoming REST DTO to process
     * @return the REST DTO response corresponding to the processed domain output
     */
    public Uni<DtoOut> remoteProcess(DtoIn dtoRequest) {
        long startNanos = System.nanoTime();
        String serviceName = getServiceName();
        DomainIn entity = fromDto(dtoRequest);
        Uni<DomainOut> processedResult = getService().process(entity);
        return processedResult
            .onItem().transform(this::toDto)
            .onTermination().invoke((item, failure, cancelled) -> {
                Throwable resolved = cancelled
                    ? new CancellationException("HTTP server call cancelled")
                    : failure;
                org.pipelineframework.telemetry.HttpMetrics.recordHttpServer(
                    serviceName, "process", resolved, startNanos);
            });
    }
}
