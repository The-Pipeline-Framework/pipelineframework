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

package org.pipelineframework.plugin.repository;

import java.util.List;
import java.util.Optional;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import io.quarkus.arc.ClientProxy;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.pipelineframework.repository.PayloadReference;
import org.pipelineframework.repository.RepositoryProvider;
import org.pipelineframework.repository.RepositoryReadResult;
import org.pipelineframework.repository.RepositoryWriteRequest;

@ApplicationScoped
@Unremovable
public class RepositoryManager {

    private static final Logger LOG = Logger.getLogger(RepositoryManager.class);

    private List<RepositoryProvider> providers = List.of();

    @Inject
    Instance<RepositoryProvider> providerInstance;

    @ConfigProperty(name = "pipeline.repository.provider")
    Optional<String> providerName;

    @ConfigProperty(name = "pipeline.repository.provider.class")
    Optional<String> providerClassName;

    @PostConstruct
    void init() {
        providers = providerInstance == null ? List.of() : providerInstance.stream().toList();
        LOG.infof("Initialised %s repository providers", providers.size());
    }

    public Uni<PayloadReference> store(RepositoryWriteRequest request) {
        RepositoryProvider provider = resolveProvider(null);
        return provider.store(request);
    }

    public Uni<RepositoryReadResult> load(PayloadReference reference) {
        RepositoryProvider provider = resolveProvider(reference);
        return provider.load(reference);
    }

    public Uni<Boolean> delete(PayloadReference reference) {
        RepositoryProvider provider = resolveProvider(reference);
        return provider.delete(reference);
    }

    public Uni<Boolean> exists(PayloadReference reference) {
        RepositoryProvider provider = resolveProvider(reference);
        return provider.exists(reference);
    }

    RepositoryProvider resolveProvider(PayloadReference reference) {
        if (providers == null || providers.isEmpty()) {
            throw new IllegalStateException("No repository providers available");
        }
        if (providerClassName.isPresent() && !providerClassName.get().isBlank()) {
            String configured = providerClassName.get().trim();
            return providers.stream()
                .filter(provider -> providerClass(provider).getName().equals(configured)
                    || providerClass(provider).getSimpleName().equals(configured))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "No repository provider matches pipeline.repository.provider.class=" + configured));
        }
        String desiredProvider = reference != null && reference.provider() != null
            ? reference.provider()
            : providerName.orElse(null);
        if (desiredProvider != null && !desiredProvider.isBlank()) {
            String configured = desiredProvider.trim();
            return providers.stream()
                .filter(provider -> configured.equalsIgnoreCase(provider.providerName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "No repository provider matches provider=" + configured));
        }
        if (providers.size() == 1) {
            return providers.get(0);
        }
        throw new IllegalStateException("Multiple repository providers found; set pipeline.repository.provider");
    }

    private Class<?> providerClass(RepositoryProvider provider) {
        Object unwrapped = provider instanceof ClientProxy ? ClientProxy.unwrap(provider) : provider;
        return unwrapped == null ? provider.getClass() : unwrapped.getClass();
    }
}
