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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import static org.junit.jupiter.api.Assertions.*;

class SpringRuntimeAdaptersAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SpringRuntimeAdaptersAutoConfiguration.class));

    @AfterEach
    void resetAdapters() {
        RuntimeAdapters.resetForTests();
    }

    @Test
    void autoConfigurationRegistersSpringRuntimeAdapters() {
        contextRunner
            .withUserConfiguration(SingleBeanConfig.class)
            .run(context -> {
                assertTrue(context.containsBean("springRuntimeAdapterBootstrap"));
                assertEquals("primary", RuntimeAdapters.resolveBean(SampleService.class).orElseThrow().name());
                assertEquals("config-value", RuntimeAdapters.resolveConfig(SampleConfig.class).orElseThrow().value());
            });
    }

    @Test
    void beanLookupReturnsEmptyForAmbiguousBeansWithoutPrimary() {
        contextRunner
            .withUserConfiguration(AmbiguousBeanConfig.class)
            .run(context -> assertTrue(RuntimeAdapters.resolveBean(SampleService.class).isEmpty()));
    }

    @Test
    void executionContextCarrierStoresTypedValues() {
        contextRunner.run(context -> {
            RuntimeAdapters.setExecutionContext("tenant", "tenant-1");

            assertEquals("tenant-1", RuntimeAdapters.executionContext("tenant", String.class));
            assertNull(RuntimeAdapters.executionContext("tenant", Integer.class));

            RuntimeAdapters.clearExecutionContext("tenant");
            assertNull(RuntimeAdapters.executionContext("tenant", String.class));
        });
    }

    @Test
    void schedulerAndBlockingRuntimeCompleteWork() {
        contextRunner.run(context -> {
            assertEquals("scheduled", RuntimeAdapters.schedule(() -> "scheduled")
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS));
            assertEquals("blocking", RuntimeAdapters.executeBlocking(() -> "blocking", false)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS));
            assertTrue(RuntimeAdapters.executeBlocking(() -> Thread.currentThread().isVirtual(), true)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS));
        });
    }

    @Test
    void transactionBoundaryRunsDirectlyWhenNoTransactionManagerExists() {
        contextRunner.run(context -> assertEquals("direct", RuntimeAdapters.runInTransaction(() -> "direct")));
    }

    @Test
    void transactionBoundaryUsesSpringTransactionManagerWhenPresent() {
        contextRunner
            .withUserConfiguration(TransactionManagerConfig.class)
            .run(context -> {
                CountingTransactionManager transactionManager = context.getBean(CountingTransactionManager.class);

                assertEquals("transactional", RuntimeAdapters.runInTransaction(() -> "transactional"));
                assertEquals(1, transactionManager.begins.get());
                assertEquals(1, transactionManager.commits.get());
                assertEquals(0, transactionManager.rollbacks.get());
            });
    }

    @Test
    void eventBusAndWorkDispatcherPublishSpringEvents() {
        contextRunner
            .withUserConfiguration(EventListenerConfig.class)
            .run(context -> {
                RecordingEvents events = context.getBean(RecordingEvents.class);

                RuntimeAdapters.eventBusBridge().publish("orders.ready", "payload");
                RuntimeAdapters.workDispatcher().enqueue("work-item").toCompletableFuture().get(5, TimeUnit.SECONDS);

                assertEquals(List.of(new SpringRuntimeEvent("orders.ready", "payload")), events.runtimeEvents);
                assertEquals(List.of(new SpringRuntimeWorkEvent("work-item")), events.workEvents);
            });
    }

    @Test
    void unaryPipelineRunnerExecutesSpringStepBeans() {
        contextRunner
            .withUserConfiguration(UnaryStepConfig.class)
            .run(context -> {
                SpringUnaryPipelineRunner runner = context.getBean(SpringUnaryPipelineRunner.class);

                assertEquals(2, runner.stepCount());
                assertEquals("HELLO!", runner.run("hello").toCompletableFuture().get(5, TimeUnit.SECONDS));
            });
    }

    @Test
    void contextShutdownResetsRuntimeAdapters() {
        contextRunner
            .withUserConfiguration(SingleBeanConfig.class)
            .run(context -> assertTrue(RuntimeAdapters.resolveBean(SampleService.class).isPresent()));

        assertTrue(RuntimeAdapters.resolveBean(SampleService.class).isEmpty());
    }

    @Test
    void closingLatestContextRestoresPreviousRuntimeAdapters() {
        AnnotationConfigApplicationContext firstContext = springContextWithService("first");
        AnnotationConfigApplicationContext secondContext = springContextWithService("second");
        SpringRuntimeAdapterBootstrap firstBootstrap = new SpringRuntimeAdapterBootstrap(firstContext, firstContext, null, null);
        SpringRuntimeAdapterBootstrap secondBootstrap = new SpringRuntimeAdapterBootstrap(secondContext, secondContext, null, null);
        boolean firstCleaned = false;
        boolean secondCleaned = false;

        try {
            firstBootstrap.afterPropertiesSet();
            assertEquals("first", RuntimeAdapters.resolveBean(SampleService.class).orElseThrow().name());

            secondBootstrap.afterPropertiesSet();
            assertEquals("second", RuntimeAdapters.resolveBean(SampleService.class).orElseThrow().name());

            secondBootstrap.destroy();
            secondContext.close();
            secondCleaned = true;

            assertEquals("first", RuntimeAdapters.resolveBean(SampleService.class).orElseThrow().name());
        } finally {
            if (!secondCleaned) {
                secondBootstrap.destroy();
                secondContext.close();
            }
            if (!firstCleaned) {
                firstBootstrap.destroy();
                firstContext.close();
                firstCleaned = true;
            }
        }

        assertTrue(RuntimeAdapters.resolveBean(SampleService.class).isEmpty());
    }

    private AnnotationConfigApplicationContext springContextWithService(String name) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(SampleService.class, () -> new SampleService(name));
        context.refresh();
        return context;
    }

    record SampleService(String name) {
    }

    record SampleConfig(String value) {
    }

    @Configuration(proxyBeanMethods = false)
    static class SingleBeanConfig {
        @Bean
        SampleService sampleService() {
            return new SampleService("primary");
        }

        @Bean
        SampleConfig sampleConfig() {
            return new SampleConfig("config-value");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class AmbiguousBeanConfig {
        @Bean
        SampleService firstService() {
            return new SampleService("first");
        }

        @Bean
        SampleService secondService() {
            return new SampleService("second");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TransactionManagerConfig {
        @Bean
        @Primary
        CountingTransactionManager transactionManager() {
            return new CountingTransactionManager();
        }
    }

    static final class CountingTransactionManager implements PlatformTransactionManager {
        private final AtomicInteger begins = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();

        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            begins.incrementAndGet();
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
            commits.incrementAndGet();
        }

        @Override
        public void rollback(TransactionStatus status) {
            rollbacks.incrementAndGet();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class EventListenerConfig {
        @Bean
        RecordingEvents recordingEvents() {
            return new RecordingEvents();
        }
    }

    static final class RecordingEvents {
        private final List<SpringRuntimeEvent> runtimeEvents = new CopyOnWriteArrayList<>();
        private final List<SpringRuntimeWorkEvent> workEvents = new CopyOnWriteArrayList<>();

        @EventListener
        void onRuntimeEvent(SpringRuntimeEvent event) {
            runtimeEvents.add(event);
        }

        @EventListener
        void onWorkEvent(SpringRuntimeWorkEvent event) {
            workEvents.add(event);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UnaryStepConfig {
        @Bean
        PipelineUnaryStep<String, String> uppercaseStep() {
            return input -> CompletableFuture.completedFuture(input.toUpperCase());
        }

        @Bean
        PipelineUnaryStep<String, String> suffixStep() {
            return input -> CompletableFuture.completedFuture(input + "!");
        }
    }
}
