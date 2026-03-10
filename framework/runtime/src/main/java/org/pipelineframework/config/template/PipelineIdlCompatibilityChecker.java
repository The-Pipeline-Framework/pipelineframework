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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks whether a current normalized IDL snapshot remains compatible with a baseline snapshot.
 */
public final class PipelineIdlCompatibilityChecker {

    /**
     * Determine compatibility violations between a baseline and a current IDL snapshot.
     *
     * <p>Compares steps and messages and returns human-readable error messages describing any incompatibilities.
     *
     * @param baseline baseline snapshot to compare against
     * @param current current snapshot under test
     * @return a list of compatibility error messages; empty if there are no violations
     */
    public List<String> compare(PipelineIdlSnapshot baseline, PipelineIdlSnapshot current) {
        List<String> errors = new ArrayList<>();
        compareSteps(baseline.steps(), current.steps(), errors);
        compareMessages(baseline.messages(), current.messages(), errors);
        return errors;
    }

    /**
     * Compare step definitions between a baseline and current snapshot and record compatibility violations.
     *
     * For each baseline step, verifies the step exists in the current snapshot and that its input and output
     * message type names are unchanged; appends human-readable error messages to {@code errors} for any
     * missing steps or changed input/output message types.
     *
     * @param baselineSteps the list of step snapshots from the baseline snapshot
     * @param currentSteps the list of step snapshots from the current snapshot
     * @param errors a mutable list that will be appended with human-readable compatibility violation messages
     */
    private void compareSteps(
        List<PipelineIdlSnapshot.StepSnapshot> baselineSteps,
        List<PipelineIdlSnapshot.StepSnapshot> currentSteps,
        List<String> errors
    ) {
        Map<String, PipelineIdlSnapshot.StepSnapshot> currentByName = new LinkedHashMap<>();
        for (PipelineIdlSnapshot.StepSnapshot step : currentSteps) {
            currentByName.put(step.name(), step);
        }
        for (PipelineIdlSnapshot.StepSnapshot baselineStep : baselineSteps) {
            PipelineIdlSnapshot.StepSnapshot currentStep = currentByName.get(baselineStep.name());
            if (currentStep == null) {
                errors.add("Missing step in current IDL: " + baselineStep.name());
                continue;
            }
            if (!safeEquals(baselineStep.inputTypeName(), currentStep.inputTypeName())) {
                errors.add("Step '" + baselineStep.name() + "' changed input message from '"
                    + baselineStep.inputTypeName() + "' to '" + currentStep.inputTypeName() + "'");
            }
            if (!safeEquals(baselineStep.outputTypeName(), currentStep.outputTypeName())) {
                errors.add("Step '" + baselineStep.name() + "' changed output message from '"
                    + baselineStep.outputTypeName() + "' to '" + currentStep.outputTypeName() + "'");
            }
        }
    }

    /**
     * Compares message definitions from a baseline snapshot against a current snapshot and records any compatibility violations.
     *
     * @param baselineMessages map of message name to baseline message snapshot
     * @param currentMessages  map of message name to current message snapshot
     * @param errors            list to which human-readable compatibility error messages will be appended
     */
    private void compareMessages(
        Map<String, PipelineIdlSnapshot.MessageSnapshot> baselineMessages,
        Map<String, PipelineIdlSnapshot.MessageSnapshot> currentMessages,
        List<String> errors
    ) {
        for (Map.Entry<String, PipelineIdlSnapshot.MessageSnapshot> entry : baselineMessages.entrySet()) {
            String messageName = entry.getKey();
            PipelineIdlSnapshot.MessageSnapshot baselineMessage = entry.getValue();
            PipelineIdlSnapshot.MessageSnapshot currentMessage = currentMessages.get(messageName);
            if (currentMessage == null) {
                errors.add("Missing message in current IDL: " + messageName);
                continue;
            }
            compareMessage(messageName, baselineMessage, currentMessage, errors);
        }
    }

    /**
     * Compare a message definition from the baseline to the current snapshot and record compatibility violations.
     *
     * Performs these checks and appends human-readable error messages to {@code errors}:
     * - Current fields that reuse baseline reserved field numbers or names.
     * - Baseline fields removed in the current message without reserving both their number and name.
     * - Field-level incompatibilities between matching fields (delegated to {@code compareField}).
     * - Current reserved numbers or names that are also used by active fields.
     *
     * @param messageName     the name of the message being compared
     * @param baselineMessage the message snapshot from the baseline
     * @param currentMessage  the message snapshot from the current snapshot
     * @param errors          a mutable list that will be populated with compatibility error messages
     */
    private void compareMessage(
        String messageName,
        PipelineIdlSnapshot.MessageSnapshot baselineMessage,
        PipelineIdlSnapshot.MessageSnapshot currentMessage,
        List<String> errors
    ) {
        Map<Integer, PipelineIdlSnapshot.FieldSnapshot> currentByNumber = new LinkedHashMap<>();
        Map<String, PipelineIdlSnapshot.FieldSnapshot> currentByName = new LinkedHashMap<>();
        for (PipelineIdlSnapshot.FieldSnapshot field : currentMessage.fields()) {
            currentByNumber.put(field.number(), field);
            currentByName.put(field.name(), field);
            if (baselineMessage.reservedNumbers().contains(field.number())) {
                errors.add("Message '" + messageName + "' reused reserved field number " + field.number());
            }
            if (baselineMessage.reservedNames().contains(field.name())) {
                errors.add("Message '" + messageName + "' reused reserved field name '" + field.name() + "'");
            }
        }

        for (PipelineIdlSnapshot.FieldSnapshot baselineField : baselineMessage.fields()) {
            PipelineIdlSnapshot.FieldSnapshot currentField = currentByNumber.get(baselineField.number());
            if (currentField == null) {
                boolean reservedNumber = currentMessage.reservedNumbers().contains(baselineField.number());
                boolean reservedName = currentMessage.reservedNames().contains(baselineField.name());
                if (!reservedNumber || !reservedName) {
                    errors.add("Message '" + messageName + "' removed field '" + baselineField.name()
                        + "' without reserving both its number and name");
                }
                continue;
            }
            compareField(messageName, baselineField, currentField, errors);
        }

        for (Integer reservedNumber : currentMessage.reservedNumbers()) {
            if (currentByNumber.containsKey(reservedNumber)) {
                errors.add("Message '" + messageName + "' declares reserved number " + reservedNumber
                    + " while also using it on an active field");
            }
        }
        for (String reservedName : currentMessage.reservedNames()) {
            if (currentByName.containsKey(reservedName)) {
                errors.add("Message '" + messageName + "' declares reserved name '" + reservedName
                    + "' while also using it on an active field");
            }
        }
    }

    /**
     * Compares the properties of a single baseline field against its current counterpart and records any compatibility violations.
     *
     * @param messageName     the name of the message containing the field
     * @param baselineField   the field snapshot from the baseline snapshot
     * @param currentField    the corresponding field snapshot from the current snapshot
     * @param errors          mutable list to which human-readable error messages will be appended for any detected incompatibilities
     */
    private void compareField(
        String messageName,
        PipelineIdlSnapshot.FieldSnapshot baselineField,
        PipelineIdlSnapshot.FieldSnapshot currentField,
        List<String> errors
    ) {
        if (!safeEquals(baselineField.name(), currentField.name())) {
            errors.add("Message '" + messageName + "' changed field name at number " + baselineField.number()
                + " from '" + baselineField.name() + "' to '" + currentField.name() + "'");
        }
        if (!safeEquals(baselineField.canonicalType(), currentField.canonicalType())) {
            errors.add("Message '" + messageName + "' changed canonical type for field '" + baselineField.name()
                + "' from '" + baselineField.canonicalType() + "' to '" + currentField.canonicalType() + "'");
        }
        if (!safeEquals(baselineField.messageRef(), currentField.messageRef())) {
            errors.add("Message '" + messageName + "' changed message reference for field '" + baselineField.name()
                + "' from '" + baselineField.messageRef() + "' to '" + currentField.messageRef() + "'");
        }
        if (!safeEquals(baselineField.keyType(), currentField.keyType())
            || !safeEquals(baselineField.valueType(), currentField.valueType())) {
            errors.add("Message '" + messageName + "' changed map structure for field '" + baselineField.name() + "'");
        }
        if (baselineField.repeated() != currentField.repeated()) {
            errors.add("Message '" + messageName + "' changed repeated structure for field '" + baselineField.name() + "'");
        }
        // Optionality changes are treated as breaking so the normalized contract stays conservative
        // across protobuf, generated bindings, and transport-specific presence semantics.
        if (baselineField.optional() != currentField.optional()) {
            errors.add("Message '" + messageName + "' changed optionality for field '" + baselineField.name() + "'");
        }
    }

    /**
     * Compare two objects for equality using null-safe semantics.
     *
     * @return `true` if both objects are null or `left.equals(right)`, `false` otherwise.
     */
    private boolean safeEquals(Object left, Object right) {
        if (left == null) {
            return right == null;
        }
        return left.equals(right);
    }
}
