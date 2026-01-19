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

import io.smallrye.mutiny.Multi;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

/**
 * Base adapter for REST resources that accept and return streaming DTOs.
 *
 * @param <DtoIn> The DTO input type
 * @param <DtoOut> The DTO output type
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 */
public abstract class RestReactiveBidirectionalStreamingServiceAdapter<DtoIn, DtoOut, DomainIn, DomainOut> {

    /**
     * Default constructor for RestReactiveBidirectionalStreamingServiceAdapter.
     */
    public RestReactiveBidirectionalStreamingServiceAdapter() {
    }

    /**
     * Provide the bidirectional streaming service that processes domain inputs into domain outputs.
     *
     * @return the {@link ReactiveBidirectionalStreamingService} instance that processes {@code DomainIn} into {@code DomainOut}
     */
    protected abstract ReactiveBidirectionalStreamingService<DomainIn, DomainOut> getService();

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
     * Process a REST request stream through the bidirectional streaming domain service.
     *
     * Converts the REST DTO stream to a domain stream, invokes the bidirectional service,
     * and converts each resulting domain output back to a REST DTO.
     *
     * @param dtoRequests the incoming REST DTO stream to process
     * @return the stream of REST DTO responses corresponding to processed domain outputs
     */
    public Multi<DtoOut> remoteProcess(Multi<DtoIn> dtoRequests) {
        Multi<DomainIn> domainStream = dtoRequests.onItem().transform(this::fromDto);
        Multi<DomainOut> processedResult = getService().process(domainStream);
        return processedResult.onItem().transform(this::toDto);
    }
}
