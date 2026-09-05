package org.pipelineframework.host.gmail;

import java.util.Optional;

record ConnectionState(long revision, GmailConnections.Phase phase, long changedAt,
                       Optional<GmailAuthorization.Grant> grant, Optional<Challenge> challenge) {
    ConnectionState {
        java.util.Objects.requireNonNull(phase);
        java.util.Objects.requireNonNull(grant);
        java.util.Objects.requireNonNull(challenge);
        if (revision < 0 || changedAt < 0
            || ((phase == GmailConnections.Phase.READY || phase == GmailConnections.Phase.REFRESHING) && grant.isEmpty())
            || ((phase == GmailConnections.Phase.CONNECTING) != challenge.isPresent())
            || ((phase == GmailConnections.Phase.DISCONNECTED || phase == GmailConnections.Phase.REQUIRES_REAUTHORIZATION) && grant.isPresent())) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }
    record Challenge(String stateHash, String browserHash, String actor, String verifier, long expiresAt) {
        @Override
        public String toString() { return "Challenge[redacted]"; }
    }

    ConnectionState next(GmailConnections.Phase nextPhase, long now,
                         Optional<GmailAuthorization.Grant> nextGrant, Optional<Challenge> nextChallenge) {
        return new ConnectionState(revision + 1, nextPhase, now, nextGrant, nextChallenge);
    }

    @Override
    public String toString() { return "ConnectionState[revision=" + revision + ",phase=" + phase + "]"; }
}
