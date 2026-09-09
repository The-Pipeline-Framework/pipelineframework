package org.pipelineframework.connector.http;

/** Explicit wire target for the existing Command dispatch identity. */
public record HttpProviderIdempotencyKeyTarget(HttpParameterLocation location, String name) {
    public HttpProviderIdempotencyKeyTarget {
        if (location != HttpParameterLocation.HEADER) {
            throw new IllegalArgumentException("initial provider idempotency-key projection supports HEADER only");
        }
        name = HttpParameterPin.requireText(name, "provider idempotency-key header");
        if (HttpParameterPin.reservedHeader(name)) {
            throw new IllegalArgumentException("provider idempotency-key target uses a reserved header");
        }
    }
}
