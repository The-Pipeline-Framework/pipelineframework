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

public class TestEntity {

    // Getters and setters
    private String name;
    private String description;

    public TestEntity() {
        super();
    }

    /**
     * Constructs a TestEntity with the given name and description.
     *
     * @param name the entity's name
     * @param description a short description of the entity
     */
    public TestEntity(String name, String description) {
        super();
        this.name = name;
        this.description = description;
    }

    /**
     * Retrieves the entity's name.
     *
     * @return the name of the entity, or {@code null} if not set
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the entity's description.
     *
     * @return the description value, or null if not set
     */
    public String getDescription() {
        return description;
    }
}