package org.pipelineframework.host.gmail;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.ws.rs.CookieParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

/**
 * Explicitly mounted host endpoints. Subclass with an application @Path and injected constructor.
 * The host must authenticate users and implement Access; no endpoints or resolver are auto-installed.
 */
@Produces("application/json")
public class GmailConnectionResource {
    public enum Action { CONNECT, CALLBACK, STATUS, DISCONNECT }
    public record Authority(ConnectionKey key, String actor) {
        public Authority {
            Objects.requireNonNull(key);
            Objects.requireNonNull(actor);
            if (actor.isBlank()) { throw new IllegalArgumentException("Actor required"); }
        }
    }
    @FunctionalInterface
    public interface Access {
        /** Authorize management and derive tenant/reference from host authority, never request payloads. */
        Authority authorize(SecurityContext security, Action action);
    }

    private static final String COOKIE = "__Host-tpf-gmail-oauth";
    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(GmailConnectionResource.class.getName());
    private final GmailConnections connections;
    private final Access access;
    private final String origin;

    public GmailConnectionResource(GmailConnections connections, Access access, URI origin) {
        this.connections = Objects.requireNonNull(connections);
        this.access = Objects.requireNonNull(access);
        this.origin = origin.toString();
        if (!"https".equals(origin.getScheme()) || origin.getHost() == null
            || origin.getRawUserInfo() != null || !origin.getRawPath().isEmpty()
            || origin.getRawQuery() != null || origin.getRawFragment() != null) {
            throw new IllegalArgumentException("Configure an exact HTTPS browser origin without path");
        }
    }

    @POST
    @Path("connect")
    public CompletionStage<Response> connect(@Context SecurityContext security, @HeaderParam("Origin") String requestOrigin) {
        return invoke(security, Action.CONNECT, () -> {
            sameOrigin(requestOrigin);
            Authority authority = access.authorize(security, Action.CONNECT);
            return connections.begin(authority.key(), authority.actor()).thenApply(start ->
                response(303).location(start.redirect()).cookie(cookie(start.browserSecret(), 600)).build());
        });
    }

    @GET
    @Path("callback")
    public CompletionStage<Response> callback(@Context SecurityContext security,
        @QueryParam("state") String state, @QueryParam("code") String code,
        @CookieParam(COOKIE) String browser) {
        return invoke(security, Action.CALLBACK, () -> {
            Authority authority = access.authorize(security, Action.CALLBACK);
            return connections.complete(authority.key(), authority.actor(), state, browser, code)
                .thenApply(status -> response(200).entity(status).cookie(cookie("", 0)).build());
        });
    }

    @GET
    @Path("status")
    public CompletionStage<Response> status(@Context SecurityContext security) {
        return invoke(security, Action.STATUS, () -> {
            Authority authority = access.authorize(security, Action.STATUS);
            return connections.status(authority.key()).thenApply(status -> response(200).entity(status).build());
        });
    }

    @POST
    @Path("disconnect")
    public CompletionStage<Response> disconnect(@Context SecurityContext security, @HeaderParam("Origin") String requestOrigin) {
        return invoke(security, Action.DISCONNECT, () -> {
            sameOrigin(requestOrigin);
            Authority authority = access.authorize(security, Action.DISCONNECT);
            return connections.disconnect(authority.key()).thenApply(status -> response(200).entity(status)
                .cookie(cookie("", 0)).build());
        });
    }

    private CompletionStage<Response> invoke(SecurityContext security, Action action,
        java.util.function.Supplier<CompletionStage<Response>> operation) {
        try {
            if (security == null || security.getUserPrincipal() == null || !security.isSecure()) {
                return CompletableFuture.completedStage(response(403).build());
            }
            return operation.get().exceptionally(failure -> failed(failure, action));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedStage(failed(failure, action));
        }
    }

    private Response failed(Throwable failure, Action action) {
        Throwable cause = failure instanceof java.util.concurrent.CompletionException
            ? java.util.Optional.ofNullable(failure.getCause()).orElse(failure) : failure;
        int code = cause instanceof ConnectionFailure connection ? switch (connection.reason()) {
            case INVALID_CALLBACK -> 400;
            case FORBIDDEN -> 403;
            case CONFLICT -> 409;
            case REAUTHORIZE -> 409;
            default -> 503;
        } : 500;
        if (!(cause instanceof ConnectionFailure)) {
            LOG.log(java.util.logging.Level.SEVERE, "Gmail connection action {0} failed ({1})",
                new Object[] {action, cause.getClass().getName()});
        }
        // Invalid callbacks do not clear a valid outstanding browser transaction.
        return response(code).build();
    }

    private void sameOrigin(String supplied) {
        if (!origin.equals(supplied)) { throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN); }
    }

    private NewCookie cookie(String value, int age) {
        return new NewCookie.Builder(COOKIE).value(value).path("/").secure(true).httpOnly(true)
            .sameSite(NewCookie.SameSite.LAX).maxAge(age).build();
    }

    private Response.ResponseBuilder response(int status) {
        return Response.status(status).header("Cache-Control", "no-store").header("Referrer-Policy", "no-referrer");
    }
}
