package org.pipelineframework.invocation;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import org.pipelineframework.runtime.core.resilience.CircuitOpenException;
import org.pipelineframework.orchestrator.TransitionWorkerFailureException;

final class TransportBoundaryFailureClassifier {
    private static final Pattern HTTP_STATUS = Pattern.compile("\\bHTTP\\s+(\\d{3})\\b", Pattern.CASE_INSENSITIVE);

    TransportBoundaryFailureClassifier() {
    }

    TransportBoundaryFailureCategory classify(Throwable failure, boolean cancelled) {
        if (cancelled) {
            return TransportBoundaryFailureCategory.CANCELLED;
        }
        if (failure == null) {
            return TransportBoundaryFailureCategory.NONE;
        }
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        return classify(failure, seen);
    }

    private TransportBoundaryFailureCategory classify(Throwable failure, Set<Throwable> seen) {
        ArrayDeque<Throwable> queue = new ArrayDeque<>();
        queue.add(failure);
        while (!queue.isEmpty()) {
            Throwable current = queue.removeFirst();
            if (current == null || !seen.add(current)) {
                continue;
            }
            TransportBoundaryFailureCategory direct = classifyDirect(current, seen);
            if (direct != null) {
                return direct;
            }
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
            for (Throwable suppressed : current.getSuppressed()) {
                queue.add(suppressed);
            }
        }
        return TransportBoundaryFailureCategory.UNEXPECTED;
    }

    private TransportBoundaryFailureCategory classifyDirect(Throwable failure, Set<Throwable> seen) {
        if (failure instanceof CircuitOpenException) {
            return TransportBoundaryFailureCategory.CIRCUIT_OPEN;
        }
        if (failure instanceof CancellationException) {
            return TransportBoundaryFailureCategory.CANCELLED;
        }
        if (failure instanceof TimeoutException || failure instanceof HttpTimeoutException
            || failure instanceof SocketTimeoutException) {
            return TransportBoundaryFailureCategory.TIMEOUT;
        }
        if (failure instanceof StatusRuntimeException statusFailure) {
            return classifyGrpc(statusFailure.getStatus().getCode());
        }
        if (failure instanceof StatusException statusFailure) {
            return classifyGrpc(statusFailure.getStatus().getCode());
        }
        if (failure instanceof WebApplicationException webFailure && webFailure.getResponse() != null) {
            return classifyHttpStatus(webFailure.getResponse().getStatus());
        }
        if (failure instanceof JsonProcessingException) {
            return TransportBoundaryFailureCategory.MALFORMED;
        }
        if (failure instanceof ProcessingException processingFailure) {
            return classifyProcessingException(processingFailure, seen);
        }
        if (failure instanceof ConnectException || failure instanceof UnknownHostException
            || failure instanceof NoRouteToHostException) {
            return TransportBoundaryFailureCategory.UNAVAILABLE;
        }
        if (failure instanceof TransitionWorkerFailureException) {
            return classifyTransitionWorkerMessage(failure.getMessage());
        }
        return null;
    }

    private TransportBoundaryFailureCategory classifyGrpc(Status.Code code) {
        return switch (code) {
            case DEADLINE_EXCEEDED -> TransportBoundaryFailureCategory.TIMEOUT;
            case CANCELLED -> TransportBoundaryFailureCategory.CANCELLED;
            case UNAUTHENTICATED, PERMISSION_DENIED -> TransportBoundaryFailureCategory.AUTH;
            case UNAVAILABLE -> TransportBoundaryFailureCategory.UNAVAILABLE;
            case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> TransportBoundaryFailureCategory.PROTOCOL;
            case INTERNAL, DATA_LOSS -> TransportBoundaryFailureCategory.REMOTE_SERVER;
            default -> TransportBoundaryFailureCategory.UNEXPECTED;
        };
    }

    private TransportBoundaryFailureCategory classifyHttpStatus(int status) {
        if (status == HttpURLConnection.HTTP_UNAUTHORIZED || status == HttpURLConnection.HTTP_FORBIDDEN) {
            return TransportBoundaryFailureCategory.AUTH;
        }
        if (status == HttpURLConnection.HTTP_CLIENT_TIMEOUT) {
            return TransportBoundaryFailureCategory.TIMEOUT;
        }
        if (status == HttpURLConnection.HTTP_BAD_GATEWAY
            || status == HttpURLConnection.HTTP_UNAVAILABLE
            || status == HttpURLConnection.HTTP_GATEWAY_TIMEOUT) {
            return TransportBoundaryFailureCategory.UNAVAILABLE;
        }
        if (status >= 400 && status < 500) {
            return TransportBoundaryFailureCategory.PROTOCOL;
        }
        if (status >= 500) {
            return TransportBoundaryFailureCategory.REMOTE_SERVER;
        }
        return TransportBoundaryFailureCategory.UNEXPECTED;
    }

    private TransportBoundaryFailureCategory classifyProcessingException(ProcessingException failure, Set<Throwable> seen) {
        Throwable cause = failure.getCause();
        if (cause == null) {
            return TransportBoundaryFailureCategory.UNAVAILABLE;
        }
        TransportBoundaryFailureCategory causeCategory = classify(cause, seen);
        return causeCategory == TransportBoundaryFailureCategory.UNEXPECTED
            ? TransportBoundaryFailureCategory.UNAVAILABLE
            : causeCategory;
    }

    private TransportBoundaryFailureCategory classifyTransitionWorkerMessage(String message) {
        String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
        Matcher httpStatus = HTTP_STATUS.matcher(lower);
        if (httpStatus.find()) {
            return classifyHttpStatus(Integer.parseInt(httpStatus.group(1)));
        }
        if (lower.contains("timed out") || lower.contains("timeout")) {
            return TransportBoundaryFailureCategory.TIMEOUT;
        }
        if (lower.contains("malformed")) {
            return TransportBoundaryFailureCategory.MALFORMED;
        }
        if (lower.contains("unsupported protocol") || lower.contains("unsupported payload")
            || lower.contains("protocol envelope")) {
            return TransportBoundaryFailureCategory.PROTOCOL;
        }
        if (lower.contains("signature") || lower.contains("shared-secret") || lower.contains("secret")) {
            return TransportBoundaryFailureCategory.AUTH;
        }
        return TransportBoundaryFailureCategory.UNEXPECTED;
    }
}
