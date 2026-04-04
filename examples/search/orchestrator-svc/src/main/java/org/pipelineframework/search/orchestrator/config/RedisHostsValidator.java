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

    @ConfigProperty(name = "pipeline.cache.provider")
    Optional<String> cacheProvider;

    /**
     * Validates that the `quarkus.redis.hosts` configuration is provided when running in production.
     *
     * Redis host validation only runs when `pipeline.cache.provider=redis`. If the cache provider is absent or blank,
     * this validator treats Redis as unselected and skips the check, so production deployments that rely on Redis must
     * set `pipeline.cache.provider=redis` explicitly.
     *
     * @throws IllegalStateException if the active profile is "prod", Redis is selected, and `quarkus.redis.hosts` is absent or empty
     */
    @PostConstruct
    void validate() {
        java.util.List<String> profiles = ConfigUtils.getProfiles();
        boolean isProd = profiles.stream().anyMatch(profile -> "prod".equalsIgnoreCase(profile));
        boolean isBlankOrMissing = redisHosts.map(String::isBlank).orElse(true);
        boolean redisSelected = cacheProvider
            .map(String::trim)
            .map("redis"::equalsIgnoreCase)
            .orElse(false);
        if (isProd && redisSelected && isBlankOrMissing) {
            throw new IllegalStateException(
                "Missing required config: quarkus.redis.hosts (set REDIS_HOSTS in production).");
        }
    }
}
