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

package org.pipelineframework.mapper;

/**
 * Domain-centric mapper between an internal (domain) type and one external representation.
 *
 * @param <Domain> internal/domain type used by step services
 * @param <External> external transport/operator representation type
 */
public interface Mapper<Domain, External> {
    /**
     * Converts an external representation into the internal domain representation.
     *
     * @param external external input object
     * @return mapped internal domain object
     */
    Domain fromExternal(External external);

    /**
     * Convert a domain object into its external representation.
     *
     * @param domain the domain object to convert
     * @return the external representation corresponding to the provided domain object
     */
    External toExternal(Domain domain);
}
