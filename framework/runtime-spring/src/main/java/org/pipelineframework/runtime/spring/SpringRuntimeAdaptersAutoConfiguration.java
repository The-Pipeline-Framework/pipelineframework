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

package org.pipelineframework.runtime.spring;

import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.List;
import java.util.Optional;

import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.SecretResolver;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Spring Boot auto-configuration that installs Spring implementations of TPF runtime-core adapters.
 */
@AutoConfiguration
@ConditionalOnClass(RuntimeAdapters.class)
@SuppressWarnings("removal")
public class SpringRuntimeAdaptersAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConnectorRuntimeContext connectorRuntimeContext(
        ObjectProvider<TaskExecutor> taskExecutors,
        ObjectProvider<ConnectionResolver> connectionResolvers,
        ObjectProvider<SecretResolver> secretResolvers
    ) {
        Executor executor = taskExecutors.orderedStream()
            .<Executor>map(value -> value)
            .findFirst()
            .orElse(Runnable::run);
        return ConnectorRuntimeContext.of(
            "spring",
            executor,
            Clock.systemUTC(),
            exactlyOne(connectionResolvers, "ConnectionResolver"),
            exactlyOne(secretResolvers, "SecretResolver"));
    }

    @Bean
    @ConditionalOnMissingBean
    public SpringRuntimeAdapterBootstrap springRuntimeAdapterBootstrap(
        ApplicationContext applicationContext,
        ApplicationEventPublisher eventPublisher,
        ObjectProvider<TaskExecutor> taskExecutor,
        ObjectProvider<PlatformTransactionManager> transactionManager
    ) {
        Executor executor = taskExecutor.getIfAvailable();
        return new SpringRuntimeAdapterBootstrap(
            applicationContext,
            eventPublisher,
            executor,
            transactionManager.getIfAvailable());
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineRunnerCore pipelineRunnerCore() {
        return new PipelineRunnerCore();
    }

    @Bean
    @ConditionalOnMissingBean
    public SpringPipelineRunner springPipelineRunner(
        PipelineRunnerCore pipelineRunnerCore,
        List<PipelineUnaryStep<?, ?>> pipelineSteps
    ) {
        return new SpringPipelineRunner(pipelineRunnerCore, pipelineSteps);
    }

    private static <T> Optional<T> exactlyOne(ObjectProvider<T> candidates, String label) {
        List<T> resolved = candidates.orderedStream().toList();
        if (resolved.size() > 1) {
            throw new IllegalStateException("Multiple " + label + " beans are registered: "
                + resolved.stream().map(candidate -> candidate.getClass().getName()).sorted().toList());
        }
        return resolved.stream().findFirst();
    }
}
