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

package org.pipelineframework.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

/**
 * Annotation to mark a class as a pipeline step (both client and server).
 * This annotation enables automatic generation of gRPC and REST adapters.
 */
@SuppressWarnings("unused")
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PipelineStep {
    /**
     * The input type for this pipeline step.
     * @return the input type for this pipeline step
     */
    Class<?> inputType() default Void.class;

    /**
     * The output type for this pipeline step.
     * @return the output type for this pipeline step
     */
    Class<?> outputType() default Void.class;

    /**
     * The inbound mapper class for this pipeline service/step.
     * Required when inputType is specified.
     * @return the inbound mapper class
     */
    Class<?> inboundMapper() default Void.class;

    /**
     * The outbound mapper class for this pipeline service/step.
     * Required when outputType is specified.
     * @return the outbound mapper class
     */
    Class<?> outboundMapper() default Void.class;

    /**
     * The step type class for this pipeline step. This can be any of the following
     * <p>
     * StepOneToOne
     * StepOneToMany
     * StepManyToOne
     * StepManyToMany
     * StepSideEffect
     * StepOneToOneBlocking
     * StepOneToManyBlocking
     * StepManyToOneBlocking
     * StepManyToManyBlocking
     * StepOneToOneCompletableFuture
     *
     * @return the step type class
     */
    Class<?> stepType() default Void.class;

    /**
     * The backend adapter type class for this pipeline step.
     * <p>
     * This can be any of the following:
     * GrpcReactiveServiceAdapter
     * GrpcReactiveServiceStreamingAdapter
     * GrpcReactiveServiceClientStreamingAdapter
     *
     * @return the backend adapter type class
     */
    Class<?> backendType() default Void.class;

    /**
     * Whether the entrypoint method (REST or gRPC) should run on a virtual thread (default=false)
     * @return true if virtual threads should be used, false otherwise
     */
    boolean runOnVirtualThreads() default false;

    /**
     * Specifies the plugin service type for side effect processing. When present, generates
     * both regular and plugin client/server pairs.
     * @return the plugin service type for side effect processing
     */
    Class<?> sideEffect() default Void.class;

    /**
     * Optional cache key generator override for this step.
     * @return the cache key generator class to use, or CacheKeyGenerator.class to use the default
     */
    Class<? extends CacheKeyGenerator> cacheKeyGenerator() default CacheKeyGenerator.class;

    /**
     * Declares ordering requirements for the generated client step.
     *
     * @return ordering requirement for this step
     */
    OrderingRequirement ordering() default OrderingRequirement.RELAXED;

    /**
     * Declares whether the generated client step is safe to invoke concurrently.
     *
     * @return thread safety declaration for this step
     */
    ThreadSafety threadSafety() default ThreadSafety.SAFE;
}
