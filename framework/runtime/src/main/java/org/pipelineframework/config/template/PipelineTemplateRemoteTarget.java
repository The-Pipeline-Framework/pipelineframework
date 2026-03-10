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

package org.pipelineframework.config.template;

/**
 * Remote operator target definition.
 *
 * @param url literal target URL
 * @param urlConfigKey configuration key resolved at runtime startup to the target URL
 */
public record PipelineTemplateRemoteTarget(
    String url,
    String urlConfigKey
) {
    public PipelineTemplateRemoteTarget {
        url = normalize(url);
        urlConfigKey = normalize(urlConfigKey);
        if (url == null && urlConfigKey == null) {
            throw new IllegalArgumentException("PipelineTemplateRemoteTarget requires either url or urlConfigKey");
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
