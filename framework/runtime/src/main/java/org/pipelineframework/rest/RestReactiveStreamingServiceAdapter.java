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

import org.pipelineframework.service.ReactiveStreamingService;

/**
 * Base class for streaming REST resources that bridges reactive streaming services to REST endpoints.
 * 
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 * @param <DtoOut> The DTO output type
 */
public abstract class RestReactiveStreamingServiceAdapter<DomainIn, DomainOut, DtoOut> {

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
 * Convert a domain output value into its DTO representation.
 *
 * @param domainOut the domain-level result produced by the service to convert
 * @return the DTO representation of the provided domain output
 */
    protected abstract DtoOut toDto(DomainOut domainOut);
}