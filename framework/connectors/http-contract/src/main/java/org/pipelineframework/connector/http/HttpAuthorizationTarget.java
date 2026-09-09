package org.pipelineframework.connector.http;

import java.util.Locale;
import java.util.Objects;

/** One wire field that host-resolved authorization material may populate for a selected constraint. */
public record HttpAuthorizationTarget(HttpParameterLocation location, String name) {
    public HttpAuthorizationTarget {
        location = Objects.requireNonNull(location, "HTTP authorization target location must not be null");
        if (location == HttpParameterLocation.PATH) {
            throw new IllegalArgumentException("HTTP authorization material cannot populate path parameters");
        }
        name = HttpParameterPin.requireText(name, "HTTP authorization target name");
        if (!name.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}")) {
            throw new IllegalArgumentException("HTTP authorization target has an invalid name: " + name);
        }
        if (location == HttpParameterLocation.HEADER && switch (name.toLowerCase(Locale.ROOT)) {
            case "host", "content-length", "content-type", "cookie", "connection", "expect", "upgrade",
                "transfer-encoding" -> true;
            default -> false;
        }) {
            throw new IllegalArgumentException("HTTP authorization cannot set transport header '" + name + "'");
        }
    }

    public boolean matches(HttpParameterLocation candidateLocation, String candidateName) {
        if (location != candidateLocation) return false;
        return location == HttpParameterLocation.HEADER
            ? name.equalsIgnoreCase(candidateName) : name.equals(candidateName);
    }
}
