package org.pipelineframework.examples.quickbooks.domain;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonValue;
import org.pipelineframework.type.CanonicalFieldValue;

/** Projected QuickBooks report parameters; every imported MCP property is optional. */
public record QuickBooksAgedReceivablesRequestParams(
    CanonicalFieldValue<QuickBooksAgedReceivablesRequestParamsAgingMethodValue> aging_method,
    CanonicalFieldValue<BigDecimal> days_per_aging_period,
    CanonicalFieldValue<BigDecimal> num_periods,
    CanonicalFieldValue<String> report_date
) {
    public QuickBooksAgedReceivablesRequestParams {
        Objects.requireNonNull(aging_method, "aging_method");
        Objects.requireNonNull(days_per_aging_period, "days_per_aging_period");
        Objects.requireNonNull(num_periods, "num_periods");
        Objects.requireNonNull(report_date, "report_date");
    }

    /** Explicit fast-path JSON projection used by the MCP adapter. */
    @JsonValue
    public Map<String, Object> canonicalJson() {
        Map<String, Object> values = new LinkedHashMap<>();
        putPresent(values, "aging_method", aging_method);
        putPresent(values, "days_per_aging_period", days_per_aging_period);
        putPresent(values, "num_periods", num_periods);
        putPresent(values, "report_date", report_date);
        return Collections.unmodifiableMap(values);
    }

    private static void putPresent(Map<String, Object> values, String name, CanonicalFieldValue<?> field) {
        if (field.isNull()) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        field.asOptional().ifPresent(value -> values.put(name, value));
    }
}
