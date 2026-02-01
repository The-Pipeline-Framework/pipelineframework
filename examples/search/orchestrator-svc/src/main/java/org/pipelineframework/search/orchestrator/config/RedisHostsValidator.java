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

package org.pipelineframework.search.orchestrator.config;

import java.util.Optional;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.runtime.Startup;
import io.quarkus.runtime.configuration.ConfigUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Startup
@ApplicationScoped
public class RedisHostsValidator {

    @ConfigProperty(name = "quarkus.redis.hosts")
    Optional<String> redisHosts;

    /**
     * Validates that the `quarkus.redis.hosts` configuration is provided when running in production.
     *
     * Throws an exception if the active profile equals "prod" (case-insensitive) and the Redis hosts value is missing or blank.
     *
     * @throws IllegalStateException if the active profile is "prod" and `quarkus.redis.hosts` is absent or empty
     */
    @PostConstruct
    void validate() {
        java.util.List<String> profiles = ConfigUtils.getProfiles();
        boolean isProd = profiles.contains("prod");
        boolean isBlankOrMissing = redisHosts.map(String::isBlank).orElse(true);
        if (isProd && isBlankOrMissing) {
            throw new IllegalStateException(
                "Missing required config: quarkus.redis.hosts (set REDIS_HOSTS in production).");
        }
    }
}
