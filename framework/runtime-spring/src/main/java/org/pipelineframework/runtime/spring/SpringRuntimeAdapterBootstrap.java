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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.pipelineframework.runtime.core.BeanLookup;
import org.pipelineframework.runtime.core.ConfigProvider;
import org.pipelineframework.runtime.core.EventBusBridge;
import org.pipelineframework.runtime.core.ExecutionContextCarrier;
import org.pipelineframework.runtime.core.ReactiveRuntime;
import org.pipelineframework.runtime.core.RuntimeAdapters;
import org.pipelineframework.runtime.core.SchedulerBoundary;
import org.pipelineframework.runtime.core.TransactionBoundary;
import org.pipelineframework.runtime.core.WorkDispatcher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Lifecycle bean that registers Spring implementations of TPF runtime-core adapters.
 */
public class SpringRuntimeAdapterBootstrap implements InitializingBean, DisposableBean {
    private static final String EXECUTOR_THREAD_NAME = "tpf-spring-runtime-";
    private static final Object REGISTRATION_LOCK = new Object();
    private static final Deque<SpringAdapterRegistration> ACTIVE_REGISTRATIONS = new ArrayDeque<>();

    private final ApplicationContext applicationContext;
    private final ApplicationEventPublisher eventPublisher;
    private final Executor executor;
    private final PlatformTransactionManager transactionManager;
    private final ExecutorService ownedVirtualThreadExecutor;
    private SpringAdapterRegistration registration;

    public SpringRuntimeAdapterBootstrap(
        ApplicationContext applicationContext,
        ApplicationEventPublisher eventPublisher,
        Executor executor,
        PlatformTransactionManager transactionManager
    ) {
        this.applicationContext = Objects.requireNonNull(applicationContext, "applicationContext");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher");
        this.transactionManager = transactionManager;
        this.ownedVirtualThreadExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name(EXECUTOR_THREAD_NAME, 0).factory());
        this.executor = executor == null ? ownedVirtualThreadExecutor : executor;
    }

    @Override
    public void afterPropertiesSet() {
        SpringBeanLookup beanLookup = new SpringBeanLookup(applicationContext);
        SpringAdapterRegistration nextRegistration = new SpringAdapterRegistration(
            beanLookup,
            new SpringConfigProvider(beanLookup),
            new SpringExecutionContextCarrier(),
            new SpringReactiveRuntime(executor, ownedVirtualThreadExecutor),
            new SpringSchedulerBoundary(executor),
            new SpringTransactionBoundary(transactionManager),
            new SpringEventBusBridge(eventPublisher),
            new SpringWorkDispatcher(eventPublisher));

        synchronized (REGISTRATION_LOCK) {
            if (registration != null) {
                ACTIVE_REGISTRATIONS.remove(registration);
            }
            registration = nextRegistration;
            ACTIVE_REGISTRATIONS.addLast(nextRegistration);
            nextRegistration.install();
        }
    }

    @Override
    public void destroy() {
        try {
            ownedVirtualThreadExecutor.close();
        } finally {
            synchronized (REGISTRATION_LOCK) {
                if (registration != null) {
                    ACTIVE_REGISTRATIONS.remove(registration);
                    registration = null;
                    SpringAdapterRegistration activeRegistration = ACTIVE_REGISTRATIONS.peekLast();
                    if (activeRegistration == null) {
                        RuntimeAdapters.resetForTests();
                    } else {
                        activeRegistration.install();
                    }
                }
            }
        }
    }

    private record SpringAdapterRegistration(
        BeanLookup beanLookup,
        ConfigProvider configProvider,
        ExecutionContextCarrier executionContextCarrier,
        ReactiveRuntime reactiveRuntime,
        SchedulerBoundary schedulerBoundary,
        TransactionBoundary transactionBoundary,
        EventBusBridge eventBusBridge,
        WorkDispatcher workDispatcher
    ) {
        private void install() {
            RuntimeAdapters.registerBeanLookup(beanLookup);
            RuntimeAdapters.registerConfigProvider(configProvider);
            RuntimeAdapters.registerExecutionContextCarrier(executionContextCarrier);
            RuntimeAdapters.registerReactiveRuntime(reactiveRuntime);
            RuntimeAdapters.registerSchedulerBoundary(schedulerBoundary);
            RuntimeAdapters.registerTransactionBoundary(transactionBoundary);
            RuntimeAdapters.registerEventBusBridge(eventBusBridge);
            RuntimeAdapters.registerWorkDispatcher(workDispatcher);
        }
    }

    private record SpringBeanLookup(ApplicationContext applicationContext) implements BeanLookup {
        @Override
        public <T> Optional<T> get(Class<T> beanType) {
            Objects.requireNonNull(beanType, "beanType");
            try {
                return Optional.ofNullable(applicationContext.getBeanProvider(beanType).getIfAvailable());
            } catch (NoUniqueBeanDefinitionException ex) {
                return Optional.empty();
            } catch (BeansException | IllegalStateException ex) {
                return Optional.empty();
            }
        }
    }

    private record SpringConfigProvider(BeanLookup beanLookup) implements ConfigProvider {
        @Override
        public <T> Optional<T> get(Class<T> configType) {
            return beanLookup.get(configType);
        }
    }

    private static final class SpringExecutionContextCarrier implements ExecutionContextCarrier {
        private static final ThreadLocal<Map<String, Object>> CONTEXT = ThreadLocal.withInitial(HashMap::new);

        @Override
        public <T> T get(String key, Class<T> type) {
            Object value = CONTEXT.get().get(key);
            if (value == null || type == null || !type.isInstance(value)) {
                return null;
            }
            return type.cast(value);
        }

        @Override
        public void put(String key, Object value) {
            if (key == null) {
                return;
            }
            if (value == null) {
                CONTEXT.get().remove(key);
            } else {
                CONTEXT.get().put(key, value);
            }
        }

        @Override
        public void clear(String key) {
            if (key != null) {
                CONTEXT.get().remove(key);
            }
        }

        @Override
        public void clear() {
            CONTEXT.remove();
        }
    }

    private static final class SpringReactiveRuntime implements ReactiveRuntime {
        private final Executor executor;
        private final Executor virtualThreadExecutor;

        private SpringReactiveRuntime(Executor executor, Executor virtualThreadExecutor) {
            this.executor = Objects.requireNonNull(executor, "executor");
            this.virtualThreadExecutor = Objects.requireNonNull(virtualThreadExecutor, "virtualThreadExecutor");
        }

        @Override
        public <T> CompletionStage<T> executeBlocking(Supplier<T> supplier, boolean offloadToVirtualThread) {
            Objects.requireNonNull(supplier, "supplier");
            return submit(() -> supplier.get(), offloadToVirtualThread ? virtualThreadExecutor : executor);
        }
    }

    private static final class SpringSchedulerBoundary implements SchedulerBoundary {
        private final Executor executor;

        private SpringSchedulerBoundary(Executor executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
        }

        @Override
        public <T> CompletionStage<T> schedule(Callable<T> task) {
            return submit(task, executor);
        }
    }

    private static final class SpringTransactionBoundary implements TransactionBoundary {
        private final TransactionTemplate transactionTemplate;

        private SpringTransactionBoundary(PlatformTransactionManager transactionManager) {
            this.transactionTemplate = transactionManager == null ? null : new TransactionTemplate(transactionManager);
        }

        @Override
        public <T> T executeInTransaction(Callable<T> callback) {
            Objects.requireNonNull(callback, "callback");
            if (transactionTemplate == null) {
                return call(callback);
            }
            return transactionTemplate.execute(status -> call(callback));
        }
    }

    private record SpringEventBusBridge(ApplicationEventPublisher eventPublisher) implements EventBusBridge {
        @Override
        public void publish(String address, Object event) {
            eventPublisher.publishEvent(new SpringRuntimeEvent(address, event));
        }
    }

    private record SpringWorkDispatcher(ApplicationEventPublisher eventPublisher) implements WorkDispatcher {
        @Override
        public CompletionStage<Void> enqueue(Object work) {
            eventPublisher.publishEvent(new SpringRuntimeWorkEvent(work));
            return CompletableFuture.completedFuture(null);
        }
    }

    private static <T> CompletionStage<T> submit(Callable<T> task, Executor executor) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(executor, "executor");
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    result.complete(task.call());
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                }
            });
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
        return result;
    }

    private static <T> T call(Callable<T> callback) {
        try {
            return callback.call();
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
