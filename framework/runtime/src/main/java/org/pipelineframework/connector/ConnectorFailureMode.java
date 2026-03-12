/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.connector;

/**
 * Failure behavior for connector-local mapping errors.
 * These modes apply only to connector-local mapping/rejection work before downstream admission.
 */
public enum ConnectorFailureMode {
    /**
     * Propagates connector exceptions to the caller and terminates the active handoff path.
     * Use when caller-visible failure or surrounding transactional rollback is required.
     */
    PROPAGATE,

    /**
     * Logs the connector error and continues processing later records on the same stream.
     * Use when connector-local failures should not stop unrelated work in the current subscription.
     */
    LOG_AND_CONTINUE
}
