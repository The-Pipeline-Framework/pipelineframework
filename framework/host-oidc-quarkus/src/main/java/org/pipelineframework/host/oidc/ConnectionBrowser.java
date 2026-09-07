package org.pipelineframework.host.oidc;

import io.vertx.core.http.Cookie;
import io.vertx.core.http.CookieSameSite;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.util.Optional;
import jakarta.ws.rs.core.NewCookie;

/** Connection-attempt cookie, independent of Quarkus's own state and session cookies. */
final class ConnectionBrowser {
    private final String name;
    private final boolean secure;
    ConnectionBrowser(ConnectionRegistration<?> registration) {
        URI uri = registration.flowUri();
        secure = "https".equals(uri.getScheme());
        if (!secure && !("http".equals(uri.getScheme()) && ("localhost".equals(uri.getHost()) || "127.0.0.1".equals(uri.getHost())))) {
            throw new IllegalArgumentException("Connection flow requires HTTPS (HTTP loopback is for local tests only)");
        }
        name = (secure ? "__Host-" : "") + "tpf-connect-" + registration.tenantId();
    }
    String read(RoutingContext context) {
        return Optional.ofNullable(context.request().getCookie(name)).map(Cookie::getValue)
            .orElseThrow(() -> new ConnectionFailure(ConnectionFailure.Reason.INVALID_CALLBACK));
    }
    void write(RoutingContext context, String ticket) {
        context.response().addCookie(Cookie.cookie(name, ticket).setPath("/").setHttpOnly(true)
            .setSecure(secure).setSameSite(CookieSameSite.LAX).setMaxAge(600));
    }
    NewCookie cookie(String ticket, int age) {
        return new NewCookie.Builder(name).value(ticket).path("/").secure(secure).httpOnly(true)
            .sameSite(NewCookie.SameSite.LAX).maxAge(age).build();
    }
}
