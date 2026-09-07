package org.pipelineframework.examples.quickbooks.domain;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonValue;

/** Nominal imported enum wrapper for QuickBooks' aging method. */
public record QuickBooksAgedReceivablesRequestParamsAgingMethodValue(String value) {
    private static final Set<String> ALLOWED = Set.of("Current", "Report_Date");

    public QuickBooksAgedReceivablesRequestParamsAgingMethodValue {
        if (!ALLOWED.contains(value)) {
            throw new IllegalArgumentException("aging method must be Current or Report_Date");
        }
    }

    @JsonValue
    public String canonicalJson() {
        return value;
    }
}
