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

package org.pipelineframework.domain;

import lombok.Getter;

@Getter
public class TestEntity {

    /**
     * Create a PluginResolutionException with the specified detail message.
     *
     * @param message the detail message describing why the plugin could not be resolved
     */
    // Getters and setters
    private String name;
    private String description;

    public TestEntity() {
        super();
    }

    /**
     * Creates a new PluginResolutionException with the specified detail message and cause.
     *
     * @param message the detail message explaining why the plugin could not be resolved
     * @param cause the underlying cause of the resolution failure, or {@code null} if none
     */
    public TestEntity(String name, String description) {
        super();
        this.name = name;
        this.description = description;
    }
}
