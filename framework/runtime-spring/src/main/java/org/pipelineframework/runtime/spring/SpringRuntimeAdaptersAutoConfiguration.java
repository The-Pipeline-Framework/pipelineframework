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

import java.util.concurrent.Executor;
import java.util.List;

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
public class SpringRuntimeAdaptersAutoConfiguration {

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
    public SpringUnaryPipelineRunner springUnaryPipelineRunner(List<PipelineUnaryStep<?, ?>> pipelineSteps) {
        return new SpringUnaryPipelineRunner(pipelineSteps);
    }
}
