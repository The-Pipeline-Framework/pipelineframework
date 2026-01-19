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

package org.pipelineframework.context.grpc;

import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.grpc.ManagedChannelBuilder;
import io.quarkus.arc.Unremovable;
import io.quarkus.grpc.api.ChannelBuilderCustomizer;
import io.quarkus.grpc.runtime.config.GrpcClientConfiguration;

/**
 * Registers the pipeline context gRPC client interceptor.
 */
@ApplicationScoped
@Unremovable
@SuppressWarnings("rawtypes")
public class PipelineContextGrpcClientCustomizer implements ChannelBuilderCustomizer {

    /**
     * Creates a new PipelineContextGrpcClientCustomizer.
     */
    public PipelineContextGrpcClientCustomizer() {
    }

    @Inject
    PipelineContextGrpcClientInterceptor interceptor;

    @Override
    public Map<String, Object> customize(String name, GrpcClientConfiguration config, ManagedChannelBuilder builder) {
        builder.intercept(interceptor);
        return Map.of();
    }
}
