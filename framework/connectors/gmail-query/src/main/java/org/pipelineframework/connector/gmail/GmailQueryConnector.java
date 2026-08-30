package org.pipelineframework.connector.gmail;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import jakarta.enterprise.context.ApplicationScoped;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePartHeader;
import org.pipelineframework.connector.BlockingQueryOperation;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOutcome;

/** Gmail read semantics over a host-resolved, already-authenticated Google SDK client. */
@ApplicationScoped
public final class GmailQueryConnector implements ConnectorProvider<GmailProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("google.gmail");
    public static final String REQUIRED_OAUTH_SCOPE = GmailScopes.GMAIL_READONLY;
    private static final String GMAIL_USER = "me";
    private static final ConnectorConfigSchema<GmailProviderConfiguration> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(GmailProviderConfiguration.class, "google.gmail.provider", 1);

    private final AtomicReference<Optional<ActiveBinding>> activeBinding =
        new AtomicReference<>(Optional.empty());
    private final Collection<? extends ConnectorOperation> operations = List.of(
        new ListMessagesOperation(), new GetMessageOperation(), new SearchMessagesOperation());

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Optional<ConnectorConfigSchema<GmailProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return operations;
    }

    @Override
    public CompletionStage<Void> start(
        ConnectorRuntimeContext context,
        GmailProviderConfiguration configuration
    ) {
        activeBinding.set(Optional.of(new ActiveBinding(context, configuration)));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        activeBinding.set(Optional.empty());
        return CompletableFuture.completedStage(null);
    }

    private CompletionStage<AuthenticatedGmailConnection> resolve(ConnectorExecutionContext invocationContext) {
        ActiveBinding binding = activeBinding.get().orElseThrow(() ->
            new IllegalStateException("Gmail connector binding is not active"));
        if (invocationContext.tenantId().isEmpty()) {
            return CompletableFuture.failedStage(new ConnectionResolutionException(
                "Gmail connection resolution requires a tenant-aware connector invocation context"));
        }
        ConnectionResolver resolver = binding.runtimeContext().connectionResolver().orElseThrow(() ->
            new ConnectionResolutionException("No host ConnectionResolver is configured for Gmail"));
        CompletionStage<AuthenticatedGmailConnection> stage = resolver.resolve(new ConnectionResolutionRequest<>(
            binding.configuration().connection(), AuthenticatedGmailConnection.class, invocationContext));
        return Objects.requireNonNull(stage, "host ConnectionResolver returned a null stage");
    }

    private java.util.concurrent.Executor executor() {
        return activeBinding.get()
            .orElseThrow(() -> new IllegalStateException("Gmail connector binding is not active"))
            .runtimeContext().executor();
    }

    private abstract class GmailOperation<I, C, O> implements BlockingQueryOperation<I, C, O> {
        @Override
        public QueryCapabilities capabilities() {
            return QueryCapabilities.cacheable();
        }

        final CompletionStage<QueryOutcome<O>> invoke(
            QueryInvocation<I, C, O> invocation,
            GmailCall<O> call,
            boolean notFoundIsExpected
        ) {
            try {
                return resolve(invocation.executionContext())
                    .<QueryOutcome<O>>thenApplyAsync(connection -> {
                        try {
                            return new QueryOutcome.Found<>(call.execute(connection.client()));
                        } catch (IOException failure) {
                            throw new CompletionException(failure);
                        }
                    }, executor())
                    .exceptionally(failure -> failureOutcome(failure, notFoundIsExpected));
            } catch (RuntimeException failure) {
                return CompletableFuture.completedStage(failureOutcome(failure, notFoundIsExpected));
            }
        }
    }

    private final class ListMessagesOperation extends GmailOperation<
        GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> {
        private static final ConnectorConfigSchema<GmailListMessagesConfiguration> SCHEMA =
            ConnectorConfigSchema.record(GmailListMessagesConfiguration.class, "google.gmail.list.messages", 1);

        @Override
        public String id() {
            return "list.messages";
        }

        @Override
        public Optional<ConnectorConfigSchema<GmailListMessagesConfiguration>> configurationSchema() {
            return Optional.of(SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<GmailMessagePage>> query(
            QueryInvocation<GmailListMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> invocation
        ) {
            return invoke(invocation, client -> list(
                client, invocation.input(), invocation.configuration(), Optional.empty()), false);
        }
    }

    private final class SearchMessagesOperation extends GmailOperation<
        GmailSearchMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> {
        private static final ConnectorConfigSchema<GmailListMessagesConfiguration> SCHEMA =
            ConnectorConfigSchema.record(GmailListMessagesConfiguration.class, "google.gmail.search.messages", 1);

        @Override
        public String id() {
            return "search.messages";
        }

        @Override
        public Optional<ConnectorConfigSchema<GmailListMessagesConfiguration>> configurationSchema() {
            return Optional.of(SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<GmailMessagePage>> query(
            QueryInvocation<GmailSearchMessagesRequest, GmailListMessagesConfiguration, GmailMessagePage> invocation
        ) {
            return invoke(invocation, client -> list(
                client,
                new GmailListMessagesRequest(invocation.input().pageToken()),
                invocation.configuration(),
                Optional.of(invocation.input().query())), false);
        }
    }

    private final class GetMessageOperation extends GmailOperation<
        GmailGetMessageRequest, GmailGetMessageConfiguration, GmailMessage> {
        private static final ConnectorConfigSchema<GmailGetMessageConfiguration> SCHEMA =
            ConnectorConfigSchema.record(GmailGetMessageConfiguration.class, "google.gmail.get.message", 1);

        @Override
        public String id() {
            return "get.message";
        }

        @Override
        public Optional<ConnectorConfigSchema<GmailGetMessageConfiguration>> configurationSchema() {
            return Optional.of(SCHEMA);
        }

        @Override
        public CompletionStage<QueryOutcome<GmailMessage>> query(
            QueryInvocation<GmailGetMessageRequest, GmailGetMessageConfiguration, GmailMessage> invocation
        ) {
            return invoke(invocation, client -> project(client.users().messages()
                .get(GMAIL_USER, invocation.input().messageId())
                .setFormat("full")
                .execute()), true);
        }
    }

    private static GmailMessagePage list(
        Gmail client,
        GmailListMessagesRequest input,
        GmailListMessagesConfiguration configuration,
        Optional<String> query
    ) throws IOException {
        Gmail.Users.Messages.List request = client.users().messages().list(GMAIL_USER)
            .setIncludeSpamTrash(configuration.includeSpamTrash());
        configuration.maxResults().ifPresent(request::setMaxResults);
        input.pageToken().ifPresent(request::setPageToken);
        query.ifPresent(request::setQ);
        ListMessagesResponse response = request.execute();
        List<GmailMessageReference> messages = Optional.ofNullable(response.getMessages()).orElseGet(List::of)
            .stream()
            .map(message -> new GmailMessageReference(required(message.getId(), "message ID"),
                required(message.getThreadId(), "thread ID")))
            .toList();
        return new GmailMessagePage(
            messages,
            Optional.ofNullable(response.getNextPageToken()),
            Optional.ofNullable(response.getResultSizeEstimate()).orElse(0L));
    }

    private static GmailMessage project(Message message) {
        List<MessagePartHeader> sourceHeaders = message.getPayload() == null || message.getPayload().getHeaders() == null
            ? List.of()
            : message.getPayload().getHeaders();
        List<GmailMessageHeader> headers = sourceHeaders.stream()
            .map(header -> new GmailMessageHeader(
                required(header.getName(), "header name"), required(header.getValue(), "header value")))
            .toList();
        Optional<String> bodyData = message.getPayload() == null || message.getPayload().getBody() == null
            ? Optional.empty()
            : Optional.ofNullable(message.getPayload().getBody().getData());
        return new GmailMessage(
            required(message.getId(), "message ID"),
            required(message.getThreadId(), "thread ID"),
            Optional.ofNullable(message.getLabelIds()).orElseGet(List::of),
            Optional.ofNullable(message.getSnippet()),
            Optional.ofNullable(message.getInternalDate()),
            headers,
            bodyData);
    }

    private static String required(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Gmail API response omitted " + label);
        }
        return value;
    }

    private static <O> QueryOutcome<O> failureOutcome(Throwable failure, boolean notFoundIsExpected) {
        Throwable cause = unwrap(failure);
        if (cause instanceof ConnectionResolutionException) {
            return new QueryOutcome.AuthenticationRequired<>("gmail-authentication-required");
        }
        if (cause instanceof GoogleJsonResponseException googleFailure) {
            int status = googleFailure.getStatusCode();
            if (status == 401 || status == 403) {
                return new QueryOutcome.AuthenticationRequired<>("gmail-authentication-required");
            }
            if (status == 404 && notFoundIsExpected) {
                return new QueryOutcome.NotFound<>("gmail-message-not-found");
            }
            if (status == 408 || status == 429 || status >= 500) {
                return new QueryOutcome.TemporarilyUnavailable<>("gmail-temporarily-unavailable");
            }
            return new QueryOutcome.TerminalFailure<>("gmail-query-failed");
        }
        if (cause instanceof IOException) {
            return new QueryOutcome.TemporarilyUnavailable<>("gmail-temporarily-unavailable");
        }
        return new QueryOutcome.TerminalFailure<>("gmail-query-failed");
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @FunctionalInterface
    private interface GmailCall<O> {
        O execute(Gmail client) throws IOException;
    }

    private record ActiveBinding(
        ConnectorRuntimeContext runtimeContext,
        GmailProviderConfiguration configuration
    ) {
        private ActiveBinding {
            runtimeContext = Objects.requireNonNull(runtimeContext, "Gmail runtime context must not be null");
            configuration = Objects.requireNonNull(configuration, "Gmail provider configuration must not be null");
        }
    }
}
