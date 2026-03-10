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

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Contract-level validator for observer/tap policy requests.
 *
 * <p>This validator is intentionally diagnostics-first for the current cycle.
 * It enforces explicit failure for unsupported required policies and explicit
 * warning+skip behavior for unsupported optional policies.</p>
 */
public final class ObserverTapContractValidator {

    private ObserverTapContractValidator() {
    }

    public enum Policy {
        REQUIRED,
        OPTIONAL
    }

    /**
     * Validates whether a requested observer/tap policy is supported.
     *
     * @param pipelineName pipeline name for diagnostics
     * @param stepName step name for diagnostics
     * @param policy requested policy type
     * @param requestedPolicy requested observer/tap policy token
     * @param supportedPolicies set of supported policy tokens
     * @param warningSink warning sink for optional policy skip diagnostics
     * @return true when policy is supported; false only for optional unsupported policies
     * @throws IllegalStateException when required policy is unsupported
     */
    public static boolean validate(
        String pipelineName,
        String stepName,
        Policy policy,
        String requestedPolicy,
        Set<String> supportedPolicies,
        Consumer<String> warningSink
    ) {
        Objects.requireNonNull(policy, "policy must not be null");
        String normalizedRequested = normalize(requestedPolicy);
        Set<String> normalizedSupported = Objects.requireNonNull(supportedPolicies, "supportedPolicies must not be null")
            .stream()
            .filter(Objects::nonNull)
            .map(ObserverTapContractValidator::normalize)
            .collect(Collectors.toUnmodifiableSet());

        if (normalizedSupported.contains(normalizedRequested)) {
            return true;
        }

        String diagnostic = "observer/tap contract unsupported:"
            + " pipeline=" + normalize(pipelineName)
            + "; step=" + normalize(stepName)
            + "; requestedPolicy=" + policy
            + "; requested=" + normalizedRequested
            + "; supported=" + normalizedSupported;

        if (policy == Policy.REQUIRED) {
            throw new IllegalStateException(diagnostic);
        }

        Objects.requireNonNull(warningSink, "warningSink must not be null").accept(diagnostic);
        return false;
    }

    private static String normalize(String value) {
        if (value == null) {
            return "unknown";
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? "unknown" : normalized;
    }
}
