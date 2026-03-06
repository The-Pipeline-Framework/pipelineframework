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

package org.pipelineframework.reject;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/**
 * Log-based item reject sink provider.
 */
@ApplicationScoped
public class LoggingItemRejectSink implements ItemRejectSink {

    private static final Logger LOG = Logger.getLogger(LoggingItemRejectSink.class);

    @Override
    public String providerName() {
        return "log";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<Void> publish(ItemRejectEnvelope envelope) {
        return Uni.createFrom().voidItem().invoke(() -> {
            ItemRejectMetrics.record(providerName(), envelope);
            LOG.errorf(
                "Item rejected: provider=%s scope=%s step=%s execution=%s correlation=%s fingerprint=%s retriesObserved=%s retryLimit=%s finalAttempt=%s errorClass=%s errorMessage=%s",
                providerName(),
                envelope.rejectScope(),
                envelope.stepClass(),
                envelope.executionId(),
                envelope.correlationId(),
                envelope.itemFingerprint(),
                envelope.retriesObserved(),
                envelope.retryLimit(),
                envelope.finalAttempt(),
                envelope.errorClass(),
                envelope.errorMessage());
        });
    }
}
