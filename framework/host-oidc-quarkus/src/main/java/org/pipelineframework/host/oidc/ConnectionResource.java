package org.pipelineframework.host.oidc;

import io.quarkus.oidc.OidcSession;
import io.quarkus.security.Authenticated;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Host must subclass and mount with @Path. No OAuth callback implementation: Quarkus protects authorize. */
@Produces("application/json")
public class ConnectionResource {
    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(ConnectionResource.class.getName());
    private final QuarkusConnections connections;
    private final ConnectionAccess access;
    private final ConnectionBrowser browser;
    private final URI flow;
    private final String origin;
    @Inject OidcSession session;

    public ConnectionResource(QuarkusConnections connections, ConnectionAccess access) {
        this.connections = Objects.requireNonNull(connections);
        this.access = Objects.requireNonNull(access);
        flow = connections.registration().flowUri();
        origin = normalizedOrigin(flow);
        browser = new ConnectionBrowser(connections.registration());
    }
    @POST @Path("connect")
    public CompletionStage<Response> connect(@Context SecurityContext security, @HeaderParam("Origin") String suppliedOrigin) {
        return invoke(() -> {
            requireOrigin(suppliedOrigin);
            var authority = access.authorize(security, ConnectionAccess.Action.CONNECT);
            return connections.begin(authority.key(), authority.actor()).thenApply(ticket ->
                response(303).location(flow).cookie(browser.cookie(ticket, 600)).build());
        });
    }
    @GET @Path("authorize") @Authenticated
    public CompletionStage<Response> authorized(@Context RoutingContext routing) {
        return invoke(() -> {
            // Resolve the request-scoped OidcSession on the request thread before storage is offloaded.
            var logout = session.logout();
            return connections.completed(browser.read(routing)).thenCompose(completed ->
            logout.subscribeAsCompletionStage().thenApply(ignored -> completed
                ? response(200).entity("{\"phase\":\"READY\"}").cookie(browser.cookie("", 0)).build()
                // A stale session cannot satisfy a fresh attempt. Discard it and let Quarkus authorize again.
                : response(303).location(flow).build()));
        });
    }
    @GET @Path("status")
    public CompletionStage<Response> status(@Context SecurityContext security) {
        return invoke(() -> connections.status(access.authorize(security, ConnectionAccess.Action.STATUS).key())
            .thenApply(status -> response(200).entity(status).build()));
    }
    @POST @Path("disconnect")
    public CompletionStage<Response> disconnect(@Context SecurityContext security, @HeaderParam("Origin") String suppliedOrigin) {
        return invoke(() -> {
            requireOrigin(suppliedOrigin);
            return connections.disconnect(access.authorize(security, ConnectionAccess.Action.DISCONNECT).key())
                .thenApply(status -> response(200).entity(status).cookie(browser.cookie("", 0)).build());
        });
    }
    private CompletionStage<Response> invoke(Supplier<CompletionStage<Response>> work) {
        try { return work.get().exceptionally(this::failed); }
        catch (RuntimeException problem) { return CompletableFuture.completedStage(failed(problem)); }
    }
    private Response failed(Throwable problem) {
        Throwable cause = problem instanceof java.util.concurrent.CompletionException && problem.getCause() != null ? problem.getCause() : problem;
        if (!(cause instanceof ConnectionFailure)) {
            LOG.log(java.util.logging.Level.SEVERE, "Connection action failed ({0})", cause.getClass().getName());
        }
        int status = cause instanceof ConnectionFailure failure ? switch (failure.reason()) {
            case FORBIDDEN -> 403;
            case INVALID_CALLBACK -> 400;
            case CONFLICT, REAUTHORIZE -> 409;
            default -> 503;
        } : 500;
        return response(status).build();
    }
    private void requireOrigin(String supplied) {
        try {
            URI candidate = URI.create(supplied);
            if (candidate.getRawUserInfo() != null || candidate.getRawQuery() != null || candidate.getRawFragment() != null
                || (candidate.getRawPath() != null && !candidate.getRawPath().isEmpty())
                || !origin.equals(normalizedOrigin(candidate))) {
                throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
            }
        } catch (IllegalArgumentException problem) {
            throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
        }
    }
    static String normalizedOrigin(URI uri) {
        String scheme = Objects.requireNonNull(uri.getScheme()).toLowerCase(Locale.ROOT);
        String host = Objects.requireNonNull(uri.getHost()).toLowerCase(Locale.ROOT);
        int port = uri.getPort();
        if ((scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) { port = -1; }
        try { return new URI(scheme, null, host, port, null, null, null).toASCIIString(); }
        catch (URISyntaxException impossible) { throw new IllegalArgumentException("Invalid connection origin"); }
    }
    private Response.ResponseBuilder response(int status) {
        return Response.status(status).header("Cache-Control", "no-store").header("Referrer-Policy", "no-referrer");
    }
}
