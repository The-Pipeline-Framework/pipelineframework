package org.pipelineframework.host.gmail;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import com.google.api.services.gmail.Gmail;

/** Provider-local host adapter. This is not a Connector or a portable TPF OAuth SPI. */
public interface GmailAuthorization extends AutoCloseable {
    String registrationId();
    URI authorize(String state, String verifier);
    Grant exchange(String code, String verifier, Optional<Grant> previous);
    Grant refresh(Grant previous);
    Gmail client(Grant grant, Runnable beforeRequest);

    @Override
    void close();

    record Grant(String subject, String accessToken, String refreshToken, long expiresAt, Set<String> scopes) {
        public Grant {
            if (subject == null || subject.isBlank() || accessToken == null || accessToken.isBlank()
                || refreshToken == null || refreshToken.isBlank() || expiresAt <= 0) {
                throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE);
            }
            scopes = Set.copyOf(scopes);
        }

        @Override
        public String toString() { return "GmailGrant[redacted]"; }
    }
}
