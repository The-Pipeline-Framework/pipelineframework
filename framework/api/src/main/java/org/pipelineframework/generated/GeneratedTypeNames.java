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

package org.pipelineframework.generated;

/**
 * Stable names shared by the compiler and framework integrations for generated pipeline types.
 */
public final class GeneratedTypeNames {

    /** Suffix appended to generated gRPC client step classes. */
    public static final String GRPC_CLIENT_STEP_SUFFIX = "GrpcClientStep";

    /** Suffix appended to generated REST client step classes. */
    public static final String REST_CLIENT_STEP_SUFFIX = "RestClientStep";

    /** Suffix appended to generated local client step classes. */
    public static final String LOCAL_CLIENT_STEP_SUFFIX = "LocalClientStep";

    /** Suffix appended to generated gRPC service classes. */
    public static final String GRPC_SERVICE_SUFFIX = "GrpcService";

    /** Suffix appended to generated REST resource classes. */
    public static final String REST_RESOURCE_SUFFIX = "Resource";

    private GeneratedTypeNames() {
    }
}
