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
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Startup
@ApplicationScoped
public class RedisHostsValidator {

    @ConfigProperty(name = "quarkus.profile", defaultValue = "prod")
    String profile;

    @ConfigProperty(name = "quarkus.redis.hosts")
    Optional<String> redisHosts;

    @PostConstruct
    void validate() {
        if ("prod".equalsIgnoreCase(profile) && redisHosts.isEmpty()) {
            throw new IllegalStateException(
                "Missing required config: quarkus.redis.hosts (set REDIS_HOSTS in production).");
        }
    }
}
