package org.pipelineframework.host.microsoft.graph;

import java.util.Objects;
import java.util.Optional;

/** Bounded result of Microsoft Graph {@code GET /v1.0/me}; it is not connection identity. */
public record MicrosoftGraphProfile(String id, Optional<String> displayName,
        Optional<String> userPrincipalName, Optional<String> mail) {
    public MicrosoftGraphProfile {
        Objects.requireNonNull(id, "Microsoft Graph profile id must not be null");
        if (id.isBlank()) { throw new IllegalArgumentException("Microsoft Graph profile id must not be blank"); }
        displayName = Objects.requireNonNull(displayName);
        userPrincipalName = Objects.requireNonNull(userPrincipalName);
        mail = Objects.requireNonNull(mail);
    }
}
