package org.pipelineframework.connector.gmail;

import java.util.Objects;

/** Header projected from a Gmail message payload. */
public record GmailMessageHeader(String name, String value) {
    public GmailMessageHeader {
        name = Objects.requireNonNull(name, "Gmail header name must not be null");
        value = Objects.requireNonNull(value, "Gmail header value must not be null");
    }
}
