package org.pipelineframework.host.gmail;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.gmail.Gmail;
import java.util.Objects;
import org.pipelineframework.connector.gmail.AuthenticatedGmailConnection;
import org.pipelineframework.host.oidc.ConnectionRegistration;

/** Gmail SDK construction only. Quarkus acquires/renews credentials; the host owns transport shutdown. */
public final class GmailClients implements ConnectionRegistration.ClientFactory<AuthenticatedGmailConnection> {
    private final HttpTransport transport;
    private final JsonFactory json;
    public GmailClients(HttpTransport transport, JsonFactory json) {
        this.transport = Objects.requireNonNull(transport);
        this.json = Objects.requireNonNull(json);
    }
    @Override public AuthenticatedGmailConnection create(ConnectionRegistration.RequestAccess access) {
        return new AuthenticatedGmailConnection(new Gmail.Builder(transport, json, request -> {
            request.setInterceptor(outgoing -> outgoing.getHeaders().setAuthorization("Bearer " + access.accessToken()));
            request.setUnsuccessfulResponseHandler((outgoing, response, supportsRetry) -> {
                if (response.getStatusCode() == 401) { access.requiresInteraction(); }
                return false;
            });
            request.setConnectTimeout(10_000);
            request.setReadTimeout(10_000);
            request.setNumberOfRetries(0);
            request.setFollowRedirects(false);
            request.setLoggingEnabled(false);
            request.setCurlLoggingEnabled(false);
        }).setApplicationName("tpf-host-gmail").build());
    }
}
