package org.pipelineframework.connector.http;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Author-selected external-contract security alternative; it describes compatibility and carries no credential. */
public record HttpSecurityConstraint(List<HttpSecurityRequirement> requirements) {
    public HttpSecurityConstraint {
        requirements = List.copyOf(Objects.requireNonNull(requirements,
            "HTTP security requirements must not be null").stream()
            .sorted(Comparator.comparing(HttpSecurityRequirement::scheme)).toList());
        if (requirements.size() > 16) {
            throw new IllegalArgumentException("HTTP security constraint must not contain more than 16 schemes");
        }
        long distinct = requirements.stream().map(HttpSecurityRequirement::scheme).distinct().count();
        if (distinct != requirements.size()) {
            throw new IllegalArgumentException("HTTP security constraint contains duplicate schemes");
        }
    }

    public static HttpSecurityConstraint none() {
        return new HttpSecurityConstraint(List.of());
    }

    public boolean permits(HttpParameterLocation location, String name) {
        return requirements.stream().flatMap(requirement -> requirement.targets().stream())
            .anyMatch(target -> target.matches(location, name));
    }
}
