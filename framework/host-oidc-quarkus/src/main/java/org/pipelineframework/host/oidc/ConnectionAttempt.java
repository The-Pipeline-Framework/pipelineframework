package org.pipelineframework.host.oidc;

import java.util.Objects;
import java.util.Optional;

/** Encrypted browser ticket; state is generated/validated by Quarkus, not this bridge. */
record ConnectionAttempt(ConnectionKey key, String actor, String browserSecret, long revision,
                         long expiresAt, Optional<String> stateHash) {
    ConnectionAttempt {
        Objects.requireNonNull(key);
        Objects.requireNonNull(actor);
        Objects.requireNonNull(browserSecret);
        Objects.requireNonNull(stateHash);
    }
    ConnectionAttempt bind(String hash) {
        return new ConnectionAttempt(key, actor, browserSecret, revision, expiresAt, Optional.of(hash));
    }
    @Override public String toString() { return "ConnectionAttempt[redacted]"; }
}
