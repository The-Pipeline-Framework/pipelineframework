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
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
    void changingMapKeyTypeIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(
            field(1, "metadata", "map", null, "string", "string", false, false)));
        PipelineIdlSnapshot current = snapshot(List.of(
            field(1, "metadata", "map", null, "int64", "string", false, false)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed map key type")),
            "expected errors containing 'changed map key type', got: " + errors);
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

    @Test
    void changingStepOutputMessageIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshotWith(
            Map.of(
                "ChargeRequest",
                message("ChargeRequest", List.of(simpleField(1, "orderId", "uuid")), List.of(), List.of()),
                "ChargeResult",
                message("ChargeResult", List.of(simpleField(1, "paymentId", "uuid")), List.of(), List.of())),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "ChargeResult")));
        PipelineIdlSnapshot current = snapshotWith(
            baseline.messages(),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "AltChargeResult")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertEquals(1, errors.size());
        assertTrue(errors.getFirst().contains("changed output message"));
    }

    @Test
    void removingStepIsIncompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(simpleField(1, "paymentId", "uuid")));
        PipelineIdlSnapshot current = snapshotWith(baseline.messages(), List.of());

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertEquals(1, errors.size());
        assertTrue(errors.getFirst().contains("Missing step in current IDL"));
    }

    @Test
    void deprecatingFieldRemainsCompatible() {
        PipelineIdlSnapshot baseline = snapshot(List.of(simpleField(1, "paymentId", "uuid")));
        PipelineIdlSnapshot current = snapshot(List.of(field(1, "paymentId", "uuid", null, null, null, false, false, true, "string")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.isEmpty(), "Expected deprecation metadata to remain compatible, got: " + errors);
    }

    @Test
    void compatibilityResultListIsImmutable() {
        PipelineIdlSnapshot baseline = snapshot(List.of(simpleField(1, "paymentId", "uuid")));
        PipelineIdlSnapshot current = snapshot(List.of(field(1, "paymentId", "uuid", null, null, null, false, false, true, "string")));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);
        assertTrue(errors.isEmpty(), "Expected deprecation metadata to remain compatible, got: " + errors);
        assertThrows(UnsupportedOperationException.class, () -> errors.add("mutate"));
    }

    @Test
    void changingUnionVariantTypeIsIncompatible() {
        PipelineIdlSnapshot baseline = unionSnapshot(
            List.of(new PipelineIdlSnapshot.UnionVariantSnapshot("captured", "PaymentCaptured", 1)));
        PipelineIdlSnapshot current = unionSnapshot(
            List.of(new PipelineIdlSnapshot.UnionVariantSnapshot("captured", "AltPaymentCaptured", 1)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("changed variant 'captured' type")),
            "expected errors containing union variant type change, got: " + errors);
    }

    @Test
    void removingUnionVariantIsIncompatible() {
        PipelineIdlSnapshot baseline = unionSnapshot(List.of(
            new PipelineIdlSnapshot.UnionVariantSnapshot("captured", "PaymentCaptured", 1),
            new PipelineIdlSnapshot.UnionVariantSnapshot("rejected", "PaymentRejected", 2)));
        PipelineIdlSnapshot current = unionSnapshot(
            List.of(new PipelineIdlSnapshot.UnionVariantSnapshot("captured", "PaymentCaptured", 1)));

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(msg -> msg.contains("removed variant 'rejected'")),
            "expected errors containing removed union variant, got: " + errors);
    }

    @Test
    void v3AdditionsAreCompatibleButChangingGeneratedIdentityIsNot() {
        PipelineIdlSnapshot.TypeSnapshot baselineRecord = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(new PipelineIdlSnapshot.TypeFieldSnapshot(1, "paymentId", "payment_id", "uuid")),
            Optional.empty(), List.of());
        PipelineIdlSnapshot.TypeSnapshot additiveRecord = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(
                new PipelineIdlSnapshot.TypeFieldSnapshot(1, "paymentId", "payment_id", "uuid"),
                new PipelineIdlSnapshot.TypeFieldSnapshot(2, "status", "status", "string")), Optional.empty(), List.of());

        assertTrue(new PipelineIdlCompatibilityChecker().compare(v3Snapshot(baselineRecord), v3Snapshot(additiveRecord)).isEmpty());

        PipelineIdlSnapshot.TypeSnapshot renamedProtoField = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(new PipelineIdlSnapshot.TypeFieldSnapshot(1, "paymentId", "payment_identifier", "uuid")),
            Optional.empty(), List.of());
        List<String> errors = new PipelineIdlCompatibilityChecker().compare(v3Snapshot(baselineRecord), v3Snapshot(renamedProtoField));

        assertTrue(errors.stream().anyMatch(error -> error.contains("protobuf identity or type")));
    }

    @Test
    void classifiesWrapperConstraintChangesBeforeApplyingCurrentStrictPolicy() {
        PipelineTemplateWrapperConstraints baseline = new PipelineTemplateWrapperConstraints(
            Optional.of(2), Optional.empty(), Optional.of("[A-Z]+"), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());
        PipelineTemplateWrapperConstraints narrower = new PipelineTemplateWrapperConstraints(
            Optional.of(3), Optional.empty(), Optional.of("[A-Z]+"), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());
        PipelineTemplateWrapperConstraints wider = new PipelineTemplateWrapperConstraints(
            Optional.of(1), Optional.empty(), Optional.of("[A-Z]+"), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());
        PipelineTemplateWrapperConstraints incomparable = new PipelineTemplateWrapperConstraints(
            Optional.of(2), Optional.empty(), Optional.of("[0-9]+"), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty());

        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.UNCHANGED, baseline.classifyChangeFrom(baseline));
        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.NARROWING, narrower.classifyChangeFrom(baseline));
        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.WIDENING, wider.classifyChangeFrom(baseline));
        assertEquals(PipelineTemplateWrapperConstraints.Compatibility.INCOMPARABLE, incomparable.classifyChangeFrom(baseline));

        PipelineIdlSnapshot.TypeSnapshot prior = wrapperSnapshot(baseline);
        List<String> errors = new PipelineIdlCompatibilityChecker().compare(v3Snapshot(prior), v3Snapshot(wrapperSnapshot(narrower)));
        assertTrue(errors.stream().anyMatch(error -> error.contains("NARROWING") && error.contains("semantic compatibility")));
    }

    @Test
    void v3RejectsActiveFieldsAndVariantsThatReuseReservedWireIdentity() {
        PipelineIdlSnapshot.TypeSnapshot record = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(new PipelineIdlSnapshot.TypeFieldSnapshot(1, "paymentId", "payment_id", "uuid")),
            Optional.empty(), List.of(), List.of(1), List.of("payment_id"));
        PipelineIdlSnapshot.TypeSnapshot union = new PipelineIdlSnapshot.TypeSnapshot(
            "Outcome", "union", List.of(), Optional.empty(),
            List.of(new PipelineIdlSnapshot.TypeVariantSnapshot("approved", "Payment", "approved", 1)),
            List.of(1), List.of("approved"));
        PipelineIdlSnapshot snapshot = new PipelineIdlSnapshot(3, "App", "com.example", Map.of(), Map.of(),
            Map.of("Payment", record, "Outcome", union), List.of());

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(snapshot, snapshot);

        assertTrue(errors.stream().anyMatch(error -> error.contains("reused reserved protobuf tag 1")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("reused reserved protobuf field name 'payment_id'")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("reused reserved protobuf discriminator field 'approved'")));
    }

    @Test
    void v3RejectsDroppingOrReusingBaselineReservationsAcrossStateTransitions() {
        PipelineIdlSnapshot.TypeSnapshot baselineRecord = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(), Optional.empty(), List.of(), List.of(7), List.of("legacy_payment"));
        PipelineIdlSnapshot.TypeSnapshot baselineUnion = new PipelineIdlSnapshot.TypeSnapshot(
            "Outcome", "union", List.of(), Optional.empty(), List.of(), List.of(8), List.of("legacy_outcome"));
        PipelineIdlSnapshot baseline = new PipelineIdlSnapshot(3, "App", "com.example", Map.of(), Map.of(),
            Map.of("Payment", baselineRecord, "Outcome", baselineUnion), List.of());
        PipelineIdlSnapshot.TypeSnapshot currentRecord = new PipelineIdlSnapshot.TypeSnapshot(
            "Payment", "record", List.of(new PipelineIdlSnapshot.TypeFieldSnapshot(7, "payment", "legacy_payment", "uuid")),
            Optional.empty(), List.of());
        PipelineIdlSnapshot.TypeSnapshot currentUnion = new PipelineIdlSnapshot.TypeSnapshot(
            "Outcome", "union", List.of(), Optional.empty(),
            List.of(new PipelineIdlSnapshot.TypeVariantSnapshot("outcome", "Payment", "legacy_outcome", 8)));
        PipelineIdlSnapshot current = new PipelineIdlSnapshot(3, "App", "com.example", Map.of(), Map.of(),
            Map.of("Payment", currentRecord, "Outcome", currentUnion), List.of());

        List<String> errors = new PipelineIdlCompatibilityChecker().compare(baseline, current);

        assertTrue(errors.stream().anyMatch(error -> error.contains("removed baseline reserved protobuf tags")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("removed baseline reserved protobuf names")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("reused reserved protobuf tag 7")));
        assertTrue(errors.stream().anyMatch(error -> error.contains("reused reserved protobuf discriminator field 'legacy_outcome'")));
    }

    private PipelineIdlSnapshot v3Snapshot(PipelineIdlSnapshot.TypeSnapshot type) {
        return new PipelineIdlSnapshot(3, "App", "com.example", Map.of(), Map.of(), Map.of(type.name(), type), List.of());
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

    private PipelineIdlSnapshot unionSnapshot(List<PipelineIdlSnapshot.UnionVariantSnapshot> variants) {
        return new PipelineIdlSnapshot(
            2,
            "App",
            "com.example",
            Map.of(
                "PaymentCaptured",
                message("PaymentCaptured", List.of(simpleField(1, "orderId", "uuid")), List.of(), List.of()),
                "PaymentRejected",
                message("PaymentRejected", List.of(simpleField(1, "orderId", "uuid")), List.of(), List.of())),
            Map.of("PaymentOutcome", new PipelineIdlSnapshot.UnionSnapshot("PaymentOutcome", variants)),
            List.of(new PipelineIdlSnapshot.StepSnapshot("Charge Card", "ChargeRequest", "PaymentOutcome")));
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

    private PipelineIdlSnapshot.FieldSnapshot simpleField(int number, String name, String canonicalType) {
        return field(number, name, canonicalType, null, null, null, false, false);
    }

    private PipelineIdlSnapshot.TypeSnapshot wrapperSnapshot(PipelineTemplateWrapperConstraints constraints) {
        return new PipelineIdlSnapshot.TypeSnapshot("CurrencyCode", "wrapper", List.of(), Optional.of("string"), List.of(),
            List.of(), List.of(), constraints);
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
            protoType,
            null);
    }
}
