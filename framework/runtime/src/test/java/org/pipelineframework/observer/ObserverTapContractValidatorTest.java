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

package org.pipelineframework.observer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObserverTapContractValidatorTest {

    @Test
    void acceptsSupportedRequiredPolicy() {
        boolean accepted = ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.REQUIRED,
            "checkpoint-observer",
            Set.of("checkpoint-observer", "mid-step-tap"),
            message -> {
                throw new AssertionError("warning sink must not be called for accepted policy: " + message);
            });

        assertTrue(accepted);
    }

    @Test
    void rejectsUnsupportedRequiredPolicyWithExplicitDiagnostics() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.REQUIRED,
            "mid-step-tap",
            Set.of("checkpoint-observer"),
            message -> {
            }));

        assertTrue(ex.getMessage().contains("pipeline=checkout"));
        assertTrue(ex.getMessage().contains("step=Dispatch Assign Courier"));
        assertTrue(ex.getMessage().contains("policy=REQUIRED"));
        assertTrue(ex.getMessage().contains("requestedToken=mid-step-tap"));
        assertTrue(ex.getMessage().contains("supported=[checkpoint-observer]"));
    }

    @Test
    void skipsUnsupportedOptionalPolicyAndEmitsWarning() {
        List<String> warnings = new ArrayList<>();
        boolean accepted = ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.OPTIONAL,
            "mid-step-tap",
            Set.of("checkpoint-observer"),
            warnings::add);

        assertFalse(accepted);
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("pipeline=checkout"));
        assertTrue(warnings.get(0).contains("step=Dispatch Assign Courier"));
        assertTrue(warnings.get(0).contains("policy=OPTIONAL"));
    }

    @Test
    void rejectsBlankRequestedPolicyForRequired() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.REQUIRED,
            "   ",
            Set.of("checkpoint-observer", "unknown"),
            message -> {
            }));

        assertTrue(ex.getMessage().contains("requestedToken=unknown"));
    }

    @Test
    void ignoresBlankSupportedPolicyTokens() {
        List<String> warnings = new ArrayList<>();
        boolean accepted = ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.OPTIONAL,
            "unknown",
            Set.of("checkpoint-observer", " ", "\t"),
            warnings::add);

        assertFalse(accepted);
        assertEquals(1, warnings.size());
    }

    @Test
    void rejectsRequiredWhenSupportedSetEmpty() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ObserverTapContractValidator.validate(
            "checkout",
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.REQUIRED,
            "checkpoint-observer",
            Set.of(),
            message -> {
            }));

        assertTrue(ex.getMessage().contains("pipeline=checkout"));
        assertTrue(ex.getMessage().contains("step=Dispatch Assign Courier"));
        assertTrue(ex.getMessage().contains("policy=REQUIRED"));
        assertTrue(ex.getMessage().contains("requestedToken=checkpoint-observer"));
        assertTrue(ex.getMessage().contains("supported=[]"));
    }

    @Test
    void rejectsNullPipelineName() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ObserverTapContractValidator.validate(
            null,
            "Dispatch Assign Courier",
            ObserverTapContractValidator.Policy.REQUIRED,
            "mid-step-tap",
            Set.of("checkpoint-observer"),
            message -> {
            }));

        assertTrue(ex.getMessage().contains("pipeline=unknown"));
        assertTrue(ex.getMessage().contains("step=Dispatch Assign Courier"));
        assertTrue(ex.getMessage().contains("policy=REQUIRED"));
    }

    @Test
    void rejectsBlankStepName() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ObserverTapContractValidator.validate(
            "checkout",
            " ",
            ObserverTapContractValidator.Policy.REQUIRED,
            "mid-step-tap",
            Set.of("checkpoint-observer"),
            message -> {
            }));

        assertTrue(ex.getMessage().contains("pipeline=checkout"));
        assertTrue(ex.getMessage().contains("step=unknown"));
        assertTrue(ex.getMessage().contains("policy=REQUIRED"));
    }
}
