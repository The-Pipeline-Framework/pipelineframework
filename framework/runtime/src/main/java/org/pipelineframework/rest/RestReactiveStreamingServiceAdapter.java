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
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.telemetry.HttpMetrics;

/**
 * Base class for streaming REST resources that bridges reactive streaming services to REST endpoints.
 *
 * @param <DtoIn> The DTO input type
 * @param <DtoOut> The DTO output type
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 */
public abstract class RestReactiveStreamingServiceAdapter<DtoIn, DtoOut, DomainIn, DomainOut> {

    private volatile String cachedServiceName;

    /**
     * Initialises a RestReactiveStreamingServiceAdapter instance.
     *
     * Provided for subclassing; no initialisation logic is performed.
     */
    public RestReactiveStreamingServiceAdapter() {
    }

    /**
     * Provides the reactive streaming service that processes domain inputs into domain outputs.
     *
     * @return the {@link ReactiveStreamingService} instance that processes {@code DomainIn} into {@code DomainOut}
     */
    protected abstract ReactiveStreamingService<DomainIn, DomainOut> getService();

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
     * Process a REST request through the reactive streaming domain service.
     *
     * Converts the REST DTO to a domain input, invokes the streaming service, and converts
     * each resulting domain output back to a REST DTO.
     *
     * @param dtoRequest the incoming REST DTO to process
     * @return the stream of REST DTO responses corresponding to processed domain outputs
     */
    public Multi<DtoOut> remoteProcess(DtoIn dtoRequest) {
        long startNanos = System.nanoTime();
        String serviceName = getServiceName();
        DomainIn entity = fromDto(dtoRequest);
        Multi<DomainOut> processedResult = getService().process(entity);
        return processedResult
            .onItem().transform(this::toDto)
            .onTermination().invoke((failure, cancelled) -> {
                Throwable resolved = cancelled
                    ? new CancellationException("HTTP server call cancelled")
                    : failure;
                HttpMetrics.recordHttpServer(
                    serviceName, "process", resolved, startNanos);
            });
    }
}
