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

package org.pipelineframework.processor.ir;

/**
 * Binding that connects a pipeline step model to external adapter generation requirements.
 * This binding holds the information needed to generate an external adapter that delegates
 * to an operator service.
 *
 * @param model The pipeline step model containing semantic information from the @PipelineStep annotation
 * @param serviceName The name of the service
 * @param servicePackage The package of the service
 * @param delegateService The delegate service class that provides the actual implementation
 * @param externalMapper The external mapper class for mapping between application and operator types
 */
public record ExternalAdapterBinding(
        PipelineStepModel model,
        String serviceName,
        String servicePackage,
        String delegateService,
        String externalMapper
) implements PipelineBinding {
    
    /**
     * Creates a new ExternalAdapterBinding with the given parameters.
     *
     * @param model The pipeline step model
     * @param serviceName The service name
     * @param servicePackage The service package
     * @param delegateService The delegate service class name
     * @param externalMapper The external mapper class name (may be null)
     */
    public ExternalAdapterBinding {
        if (model == null) {
            throw new IllegalArgumentException("Model cannot be null");
        }
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException("Service name cannot be null or blank");
        }
        if (servicePackage == null || servicePackage.isBlank()) {
            throw new IllegalArgumentException("Service package cannot be null or blank");
        }
        if (delegateService == null || delegateService.isBlank()) {
            throw new IllegalArgumentException("Delegate service cannot be null or blank");
        }
        if (externalMapper != null && externalMapper.isBlank()) {
            throw new IllegalArgumentException("External mapper cannot be blank when provided");
        }
    }
}