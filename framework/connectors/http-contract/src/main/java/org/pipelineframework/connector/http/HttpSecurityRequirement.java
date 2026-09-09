package org.pipelineframework.connector.http;

import java.util.List;
import java.util.Objects;

/** One selected descriptive security capability, scopes, and permitted authorization wire targets. */
public record HttpSecurityRequirement(String scheme, List<String> scopes, List<HttpAuthorizationTarget> targets) {
    public HttpSecurityRequirement {
        scheme = HttpParameterPin.requireText(scheme, "HTTP security scheme");
        List<String> providedScopes = Objects.requireNonNull(scopes, "HTTP security scopes must not be null");
        if (providedScopes.size() > 64 || providedScopes.stream().anyMatch(scope -> scope == null || scope.isBlank())) {
            throw new IllegalArgumentException("HTTP security scopes must be a bounded list of non-blank values");
        }
        scopes = providedScopes.stream().map(String::trim).sorted().toList();
        if (scopes.stream().distinct().count() != scopes.size()) {
            throw new IllegalArgumentException("HTTP security scopes must be distinct");
        }
        List<HttpAuthorizationTarget> providedTargets = Objects.requireNonNull(
            targets, "HTTP authorization targets must not be null");
        if (providedTargets.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("HTTP authorization targets must not contain null");
        }
        targets = List.copyOf(providedTargets.stream()
            .sorted(java.util.Comparator.comparing((HttpAuthorizationTarget target) -> target.location().name())
                .thenComparing(HttpAuthorizationTarget::name)).toList());
        long distinctTargets = targets.stream().map(target -> target.location() + ":" +
            (target.location() == HttpParameterLocation.HEADER ? target.name().toLowerCase(java.util.Locale.ROOT)
                : target.name())).distinct().count();
        if (targets.size() > 16 || distinctTargets != targets.size()) {
            throw new IllegalArgumentException("HTTP authorization targets must be bounded and distinct");
        }
    }
}
