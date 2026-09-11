package org.pipelineframework.awaitable;

import java.io.InputStream;
import java.util.Objects;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ProviderCallbackActor;
import org.pipelineframework.connector.ProviderCallbackAuthenticator;
import org.pipelineframework.connector.ProviderCallbackAuthenticationRequest;
import org.pipelineframework.connector.ProviderCallbackRequest;
import org.pipelineframework.connector.http.HttpCallbackPin;
import org.pipelineframework.connector.http.HttpOperationCatalog;
import org.pipelineframework.connector.http.HttpOperationBindingCatalog;
import org.pipelineframework.connector.http.HttpWireValueValidator;
import org.pipelineframework.representation.http.HttpRepresentationBindings;

/** The sole HTTP ingress for release-pinned Command completion callbacks. */
@ApplicationScoped
@Path("/" + CommandDeferredCompletionSupport.CALLBACK_PATH + "{token}")
public class ProviderCallbackResource {
    private static final int MAX_BODY = 1_048_576;
    @Inject AwaitCoordinator coordinator;
    @Inject PipelineExecutionService executions;
    @Inject @Any Instance<ProviderCallbackAuthenticator> authenticators;

    @POST
    @Blocking
    public Uni<Response> complete(@PathParam("token") String token, @Context HttpHeaders headers, InputStream input) {
        byte[] body;
        try {
            if (token.length() > 8192 || headers.getMediaType() == null
                || !"application".equalsIgnoreCase(headers.getMediaType().getType())
                || !(headers.getMediaType().getSubtype().equalsIgnoreCase("json")
                    || headers.getMediaType().getSubtype().toLowerCase(java.util.Locale.ROOT).endsWith("+json"))) {
                return rejected();
            }
            ProviderCallbackAuthenticationRequest.boundedHeaders(headers.getRequestHeaders());
            body = input.readNBytes(MAX_BODY + 1);
            if (body.length > MAX_BODY) {
                return rejected();
            }
        } catch (java.io.IOException | IllegalArgumentException failure) {
            return rejected();
        } catch (RuntimeException failure) {
            return unavailable();
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return Uni.createFrom().deferred(() -> coordinator.resolveCallback(token, System.currentTimeMillis()))
            .emitOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultExecutor())
            .chain(record -> admit(record, token, headers, body, loader))
            .onFailure().recoverWithItem(this::failureResponse);
    }

    private Uni<Response> admit(AwaitInteractionRecord record, String token, HttpHeaders headers, byte[] body,
        ClassLoader loader) {
        if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
            return rejected();
        }
        var descriptor = coordinator.callbackDescriptor(record);
        var selection = descriptor.callback().orElseThrow();
        if (!"http.client".equals(selection.operation().providerId().value()) || selection.providerMajorVersion() != 1) {
            return unavailable();
        }
        var catalogue = HttpOperationCatalog.load(loader);
        HttpCallbackPin pin = catalogue.operations().stream()
            .filter(operation -> operation.operation().equals(selection.operation().operationId())
                && operation.kind().equals(selection.operation().kind())
                && operation.majorVersion() == selection.operation().majorVersion())
            .flatMap(operation -> operation.callbacks().stream())
            .filter(callback -> callback.descriptor().equals(selection.callback()))
            .findFirst().orElseThrow(() -> new IllegalStateException("Callback pin unavailable"));
        MediaType media = headers.getMediaType();
        if (!pin.method().equals("POST") || !MediaType.valueOf(pin.mediaType()).isCompatible(media)
            || media.getParameters().entrySet().stream().anyMatch(parameter ->
                !parameter.getKey().equalsIgnoreCase("charset") || !parameter.getValue().equalsIgnoreCase("utf-8"))) {
            return rejected();
        }
        var request = new ProviderCallbackAuthenticationRequest(new ProviderCallbackRequest(
            record.tenantId(), record.interactionId(), selection.operation(), selection.callback().id()),
            "POST", headers.getRequestHeaders(), body, pin.security().requirements().stream()
                .map(requirement -> new ProviderCallbackAuthenticationRequest.SecurityRequirement(
                    requirement.scheme(), requirement.scopes(), requirement.targets().stream()
                        .map(target -> new ProviderCallbackAuthenticationRequest.SecurityTarget(
                            target.location().name(), target.name())).toList())).toList());
        return Uni.createFrom().completionStage(() -> authenticator(selection, loader).authenticate(request))
            .emitOn(io.smallrye.mutiny.infrastructure.Infrastructure.getDefaultExecutor())
            .chain(actor -> actor.map(identity -> mapAndComplete(record, descriptor, pin, token, body, identity, loader))
                .orElseGet(this::rejected));
    }

    private ProviderCallbackAuthenticator authenticator(ConnectorCallbackSelection selection, ClassLoader loader) {
        try {
            Class<? extends ProviderCallbackAuthenticator> type = Class.forName(selection.authenticatorClass(), false, loader)
                .asSubclass(ProviderCallbackAuthenticator.class);
            return authenticators.select(type).get();
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Callback authenticator unavailable");
        }
    }

    private Uni<Response> mapAndComplete(AwaitInteractionRecord record, AwaitCompletionDescriptor descriptor,
        HttpCallbackPin pin, String token, byte[] body, ProviderCallbackActor actor, ClassLoader loader) {
        Class<?> callbackType;
        HttpRepresentationBindings representations;
        try {
            callbackType = Class.forName(descriptor.transportOutputType(), false, loader);
            representations = new HttpRepresentationBindings(HttpOperationBindingCatalog.load(loader), loader);
        } catch (Exception failure) {
            return unavailable();
        }
        Object canonical;
        try {
            var wire = PipelineJson.mapper().reader()
                .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(body);
            HttpWireValueValidator.validate(Objects.requireNonNull(wire), pin.requestSchema());
            canonical = representations.fromWire(pin.requestMappingKey(), wire, callbackType);
        } catch (Exception failure) {
            return failure instanceof IllegalArgumentException || failure instanceof com.fasterxml.jackson.core.JsonProcessingException
                || failure.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException ? rejected() : unavailable();
        }
        return executions.completeAwaitInteraction(new AwaitCompletionCommand(record.tenantId(), record.interactionId(),
            record.correlationId(), token, "callback:" + pin.id(), canonical, actor.value(), System.currentTimeMillis()))
            .map(accepted -> Response.status(pin.acknowledgementStatus()).build());
    }

    private Response failureResponse(Throwable failure) {
        Throwable classified = failure;
        for (int depth = 0; depth < 8 && (classified instanceof java.util.concurrent.CompletionException
            || classified instanceof java.util.concurrent.ExecutionException) && classified.getCause() != null; depth++) {
            classified = classified.getCause();
        }
        return Response.status(classified instanceof AwaitCompletionAdmissionFailure ? 400 : 503).build();
    }

    private Uni<Response> unavailable() {
        return Uni.createFrom().item(() -> Response.status(503).build());
    }

    private Uni<Response> rejected() {
        return Uni.createFrom().item(() -> Response.status(400).build());
    }
}
