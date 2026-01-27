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

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveStreamingClientService;

/**
 * Base adapter for REST resources that accept streaming DTO inputs and return a single DTO output.
 *
 * @param <DtoIn> The DTO input type
 * @param <DtoOut> The DTO output type
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 */
public abstract class RestReactiveStreamingClientServiceAdapter<DtoIn, DtoOut, DomainIn, DomainOut> {

    private volatile String cachedServiceName;

    /**
     * Default constructor for RestReactiveStreamingClientServiceAdapter.
     */
    public RestReactiveStreamingClientServiceAdapter() {
    }

    /**
     * Provide the streaming client service that processes domain inputs into a single domain output.
     *
     * @return the {@link ReactiveStreamingClientService} instance that processes {@code DomainIn} into {@code DomainOut}
     */
    protected abstract ReactiveStreamingClientService<DomainIn, DomainOut> getService();

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
     * @return the domain input produced from the DTO
     */
    protected abstract DomainIn fromDto(DtoIn dtoIn);

    /**
     * Convert a domain output value into its DTO representation.
     *
     * @param domainOut the domain-level result produced by the service to convert
     * @return the DTO representation of the provided domain output
     */
    protected abstract DtoOut toDto(DomainOut domainOut);

    /**
     * Process a REST request stream through the streaming client domain service.
     *
     * Converts the REST DTO stream to a domain stream, invokes the streaming client service,
     * and converts the resulting domain output back to a REST DTO.
     *
     * @param dtoRequests the incoming REST DTO stream to process
     * @return the REST DTO response corresponding to the processed domain output
     */
    public Uni<DtoOut> remoteProcess(Multi<DtoIn> dtoRequests) {
        long startNanos = System.nanoTime();
        String serviceName = getServiceName();
        Multi<DomainIn> domainStream = dtoRequests.onItem().transform(this::fromDto);
        Uni<DomainOut> processedResult = getService().process(domainStream);
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
