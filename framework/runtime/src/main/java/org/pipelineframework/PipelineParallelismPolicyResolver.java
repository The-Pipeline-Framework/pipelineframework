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

package org.pipelineframework;

import jakarta.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;
import org.pipelineframework.annotation.ParallelismHint;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ParallelismHints;
import org.pipelineframework.parallelism.ThreadSafety;

@ApplicationScoped
class PipelineParallelismPolicyResolver {

    private static final Logger logger = Logger.getLogger(PipelineParallelismPolicyResolver.class);
    static final int DEFAULT_MAX_CONCURRENCY = 128;

    enum StepParallelismType {
        ONE_TO_ONE(false),
        ONE_TO_ONE_FUTURE(false),
        ONE_TO_MANY(true);

        private final boolean autoCandidate;

        StepParallelismType(boolean autoCandidate) {
            this.autoCandidate = autoCandidate;
        }

        boolean autoCandidate() {
            return autoCandidate;
        }
    }

    ParallelismPolicy resolveParallelismPolicy(PipelineConfig pipelineConfig) {
        if (pipelineConfig == null || pipelineConfig.parallelism() == null) {
            return ParallelismPolicy.AUTO;
        }
        return pipelineConfig.parallelism();
    }

    int resolveMaxConcurrency(PipelineConfig pipelineConfig) {
        int configured = pipelineConfig != null ? pipelineConfig.maxConcurrency() : DEFAULT_MAX_CONCURRENCY;
        if (configured < 1) {
            logger.warnf("Invalid maxConcurrency=%s; using 1", configured);
            return 1;
        }
        return configured;
    }

    boolean shouldParallelize(Object step, ParallelismPolicy policy, StepParallelismType stepType) {
        OrderingRequirement orderingRequirement = OrderingRequirement.RELAXED;
        ThreadSafety threadSafety = ThreadSafety.SAFE;
        boolean hasHints = false;
        if (step instanceof ParallelismHints hints) {
            orderingRequirement = hints.orderingRequirement();
            threadSafety = hints.threadSafety();
            hasHints = true;
        } else if (step != null) {
            ParallelismHint hint = step.getClass().getAnnotation(ParallelismHint.class);
            if (hint != null) {
                orderingRequirement = hint.ordering();
                threadSafety = hint.threadSafety();
                hasHints = true;
            }
        }

        ParallelismPolicy effectivePolicy = policy == null ? ParallelismPolicy.AUTO : policy;
        String stepName = step != null ? step.getClass().getName() : "unknown";

        if (threadSafety == ThreadSafety.UNSAFE && effectivePolicy != ParallelismPolicy.SEQUENTIAL) {
            throw new IllegalStateException("Step " + stepName + " is not thread-safe; " +
                "set pipeline.parallelism=SEQUENTIAL to proceed.");
        }

        if (orderingRequirement == OrderingRequirement.STRICT_REQUIRED &&
            effectivePolicy != ParallelismPolicy.SEQUENTIAL) {
            throw new IllegalStateException("Step " + stepName + " requires strict ordering; " +
                "set pipeline.parallelism=SEQUENTIAL to proceed.");
        }

        if (effectivePolicy == ParallelismPolicy.SEQUENTIAL) {
            return false;
        }

        if (orderingRequirement == OrderingRequirement.STRICT_ADVISED) {
            if (effectivePolicy == ParallelismPolicy.AUTO) {
                logger.warnf("Step %s advises strict ordering; AUTO will run sequentially. " +
                        "Set pipeline.parallelism=PARALLEL to override.",
                    stepName);
                return false;
            }
            logger.warnf("Step %s advises strict ordering; PARALLEL overrides the advice.", stepName);
        }

        if (effectivePolicy == ParallelismPolicy.PARALLEL) {
            return true;
        }

        if (effectivePolicy == ParallelismPolicy.AUTO && hasHints
            && orderingRequirement == OrderingRequirement.RELAXED
            && threadSafety == ThreadSafety.SAFE) {
            return true;
        }

        return stepType.autoCandidate();
    }
}
