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

import org.pipelineframework.service.ReactiveService;

/**
 * Base adapter for REST resources that process domain inputs via a ReactiveService and convert outputs to DTOs.
 * 
 * @param <DomainIn> The domain input type
 * @param <DomainOut> The domain output type
 * @param <DtoOut> The DTO output type
 */
public abstract class RestReactiveServiceAdapter<DomainIn, DomainOut, DtoOut> {

    /**
     * Default constructor for RestReactiveServiceAdapter.
     */
    public RestReactiveServiceAdapter() {
    }

    /**
     * Provides the reactive service used to process domain inputs into domain outputs.
     *
     * @return the {@code ReactiveService<DomainIn, DomainOut>} instance used to process domain objects
     */
    protected abstract ReactiveService<DomainIn, DomainOut> getService();

    /**
     * Convert a processed domain object to its REST DTO representation.
     *
     * @param domainOut the processed domain model instance to convert
     * @return the DTO representation to be returned by the REST resource
     */
    protected abstract DtoOut toDto(DomainOut domainOut);
}