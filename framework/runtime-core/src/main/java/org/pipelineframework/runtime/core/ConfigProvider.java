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

package org.pipelineframework.runtime.core;

import java.util.Optional;

/**
 * Neutral contract for reading runtime configuration contracts.
 */
public interface ConfigProvider {

    /**
     * Resolves a configuration binding from the active runtime container.
     *
     * @param configType config class type
     * @param <T> config type
     * @return configuration object when available
     */
    <T> Optional<T> get(Class<T> configType);
}
