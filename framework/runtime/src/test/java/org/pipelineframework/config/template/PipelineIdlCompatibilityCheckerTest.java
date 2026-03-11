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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineIdlCompatibilityCheckerTest {

    @Test
    void additiveFieldIsCompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, false, false),
            field(2, "status", "string", null, null, null, false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.isEmpty(), "Expected additive change to be compatible, got: " + errors);
    }

    @Test
    void removedFieldRequiresReservedNumberAndName() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, false, false)));
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

    @Test
    void changingStepInputMessageIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshotWith(
            Map.of(
                "ChargeRequest",
                message("ChargeRequest", List.of(field(1, "orderId", "uuid", null, null, null, false, false)), List.of(), List.of()),
                "ChargeResult",
                message("ChargeResult", List.of(field(1, "paymentId", "uuid", null, null, null, false, false)), List.of(), List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));
        PipelineIdlSnapshot current = snapshotWith(
            baseline.messages(),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "AltChargeRequest", "ChargeResult")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertEquals(1, errors.size());
        assertTrue(errors.getFirst().contains("changed input message"));
    }

    @Test
    void changingCanonicalTypeIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "paymentId", "string", null, null, null, false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed canonical type")),
            "expected errors containing 'changed canonical type', got: " + errors);
    }

    @Test
    void changingMessageReferenceIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "customer", "message", "CustomerRef", null, null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "customer", "message", "AltCustomerRef", null, null, false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed message reference")),
            "expected errors containing 'changed message reference', got: " + errors);
    }

    @Test
    void changingMapStructureIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "metadata", "map", null, "string", "string", false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "metadata", "map", null, "string", "int64", false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed map value type")),
            "expected errors containing 'changed map value type', got: " + errors);
    }

    @Test
    void changingRepeatedStructureIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "auditTrail", "string", null, null, null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "auditTrail", "string", null, null, null, false, true)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed repeated structure")),
            "expected errors containing 'changed repeated structure', got: " + errors);
    }

    @Test
    void changingOptionalityIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "paymentId", "uuid", null, null, null, true, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed optionality")),
            "expected errors containing 'changed optionality', got: " + errors);
    }

    @Test
    void reusingReservedNumberAndNameIsIncompatible() {
        PipelineIdlSnapshot baseline = new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            Map.of(
                "ChargeResult",
                new PipelineIdlSnapshot.MessageSnapshot(
                    "ChargeResult",
                    List.of(field(1, "paymentId", "uuid", null, null, null, false, false)),
                    List.of(3),
                    List.of("legacyCode"))),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));
        PipelineIdlSnapshot current = new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            Map.of(
                "ChargeResult",
                new PipelineIdlSnapshot.MessageSnapshot(
                    "ChargeResult",
                    List.of(
                        field(1, "paymentId", "uuid", null, null, null, false, false),
                        field(3, "legacyCode", "string", null, null, null, false, false)),
                    List.of(),
                    List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("reused reserved field number")),
            "expected errors containing 'reused reserved field number', got: " + errors);
        assertTrue(errors.stream().anyMatch(msg -> msg.contains("reused reserved field name")),
            "expected errors containing 'reused reserved field name', got: " + errors);
    }

    private PipelineIdlSnapshot snapshot(List<PipelineIdlSnapshot.FieldSnapshot> fields) {
        return snapshotWith(
            Map.of(
                "ChargeResult",
                message("ChargeResult", fields, List.of(), List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));
    }

    private PipelineIdlSnapshot snapshotWith(
        Map<String, PipelineIdlSnapshot.MessageSnapshot> messages,
        List<PipelineIdlSnapshot.StepSnapshot> steps
    ) {
        return new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            messages,
            steps);
    }

    private PipelineIdlSnapshot.MessageSnapshot message(
        String name,
        List<PipelineIdlSnapshot.FieldSnapshot> fields,
        List<Integer> reservedNumbers,
        List<String> reservedNames
    ) {
        return new PipelineIdlSnapshot.MessageSnapshot(name, fields, reservedNumbers, reservedNames);
    }

    private PipelineIdlSnapshot.FieldSnapshot field(
        int number,
        String name,
        String canonicalType,
        String messageRef,
        String keyType,
        String valueType,
        boolean optional,
        boolean repeated
    ) {
        return field(number, name, canonicalType, messageRef, keyType, valueType, optional, repeated, false, "string");
    }

    private PipelineIdlSnapshot.FieldSnapshot field(
        int number,
        String name,
        String canonicalType,
        String messageRef,
        String keyType,
        String valueType,
        boolean optional,
        boolean repeated,
        boolean deprecated,
        String protoType
    ) {
        return new PipelineIdlSnapshot.FieldSnapshot(
            number,
            name,
            canonicalType,
            messageRef,
            keyType,
            valueType,
            optional,
            repeated,
            deprecated,
            protoType);
    }
}
