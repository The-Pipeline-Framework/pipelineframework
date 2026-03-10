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

package org.pipelineframework.config.template;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineIdlCompatibilityCheckerTest {

    @Test
    void additiveFieldIsCompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "paymentId", "uuid", null, false, false),
            field(2, "status", "string", null, false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.isEmpty(), "Expected additive change to be compatible, got: " + errors);
    }

    @Test
    void removedFieldRequiresReservedNumberAndName() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, false, false)));
        PipelineIdlSnapshot current = new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            Map.of(
                "ChargeResult",
                new PipelineIdlSnapshot.MessageSnapshot(
                    "ChargeResult",
                    List.of(),
                    List.of(1),
                    List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertFalse(errors.isEmpty());
        assertTrue(errors.getFirst().contains("without reserving both its number and name"));
    }

    private PipelineIdlSnapshot snapshot(List<PipelineIdlSnapshot.FieldSnapshot> fields) {
        return new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            Map.of(
                "ChargeResult",
                new PipelineIdlSnapshot.MessageSnapshot(
                    "ChargeResult",
                    fields,
                    List.of(),
                    List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));
    }

    private PipelineIdlSnapshot.FieldSnapshot field(
        int number,
        String name,
        String canonicalType,
        String messageRef,
        boolean optional,
        boolean repeated
    ) {
        return new PipelineIdlSnapshot.FieldSnapshot(
            number,
            name,
            canonicalType,
            messageRef,
            null,
            null,
            optional,
            repeated,
            false,
            "string");
    }
}
