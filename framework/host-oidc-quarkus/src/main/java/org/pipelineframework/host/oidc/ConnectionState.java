package org.pipelineframework.host.oidc;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Infrastructure state only. Never used as a pipeline value or Connector result. */
record ConnectionState(long revision, long generation, QuarkusConnections.Phase phase, long changedAt,
                       Optional<Grant> grant, Optional<Challenge> challenge) {
    ConnectionState {
        Objects.requireNonNull(phase);
        Objects.requireNonNull(grant);
        Objects.requireNonNull(challenge);
        if (revision < 0 || generation < 0 || changedAt < 0
            || ((phase == QuarkusConnections.Phase.READY || phase == QuarkusConnections.Phase.REFRESHING
                || phase == QuarkusConnections.Phase.RENEWAL_UNCERTAIN) && grant.isEmpty())
            || ((phase == QuarkusConnections.Phase.CONNECTING) != challenge.isPresent())
            || ((phase == QuarkusConnections.Phase.DISCONNECTED || phase == QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION) && grant.isPresent())) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }
    record Grant(String account, String accessToken, String refreshToken, long expiresAt, Set<String> scopes) {
        Grant {
            Objects.requireNonNull(account);
            Objects.requireNonNull(accessToken);
            Objects.requireNonNull(refreshToken);
            scopes = Set.copyOf(scopes);
            if (account.isBlank() || accessToken.isBlank() || refreshToken.isBlank() || expiresAt <= 0) {
                throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE);
            }
        }
        @Override public String toString() { return "Grant[redacted]"; }
    }
    record Challenge(String browserHash, String actor, long expiresAt) {
        @Override public String toString() { return "Challenge[redacted]"; }
    }
    ConnectionState next(QuarkusConnections.Phase nextPhase, long now, Optional<Grant> nextGrant, Optional<Challenge> nextChallenge) {
        return new ConnectionState(revision + 1, generation, nextPhase, now, nextGrant, nextChallenge);
    }
    ConnectionState replace(QuarkusConnections.Phase nextPhase, long now, Optional<Grant> nextGrant, Optional<Challenge> nextChallenge) {
        return new ConnectionState(revision + 1, generation + 1, nextPhase, now, nextGrant, nextChallenge);
    }
    @Override public String toString() { return "ConnectionState[revision=" + revision + ",phase=" + phase + "]"; }
}
