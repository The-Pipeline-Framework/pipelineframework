package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.TransitionWorkerFailureException;

class TransportBoundaryFailureClassifierTest {
    private final TransportBoundaryFailureClassifier classifier = new TransportBoundaryFailureClassifier();

    @Test
    void classifiesSuccessAndCancellation() {
        assertEquals(TransportBoundaryFailureCategory.NONE, classify(null, false));
        assertEquals(TransportBoundaryFailureCategory.CANCELLED, classify(null, true));
        assertEquals(TransportBoundaryFailureCategory.CANCELLED, classify(new CancellationException(), false));
    }

    @Test
    void classifiesRestStyleHttpFailures() {
        assertEquals(TransportBoundaryFailureCategory.AUTH, classify(rest(401), false));
        assertEquals(TransportBoundaryFailureCategory.AUTH, classify(rest(403), false));
        assertEquals(TransportBoundaryFailureCategory.TIMEOUT, classify(rest(408), false));
        assertEquals(TransportBoundaryFailureCategory.PROTOCOL, classify(rest(422), false));
        assertEquals(TransportBoundaryFailureCategory.UNAVAILABLE, classify(rest(503), false));
        assertEquals(TransportBoundaryFailureCategory.UNEXPECTED, classify(rest(500), false));
    }

    @Test
    void classifiesRestProcessingFailuresByCause() {
        assertEquals(
            TransportBoundaryFailureCategory.TIMEOUT,
            classify(new ProcessingException(new SocketTimeoutException("read timed out")), false));
        assertEquals(
            TransportBoundaryFailureCategory.UNAVAILABLE,
            classify(new ProcessingException(new NoRouteToHostException("no route")), false));
        assertEquals(TransportBoundaryFailureCategory.UNAVAILABLE, classify(new ProcessingException("client failed"), false));
    }

    @Test
    void classifiesGrpcFailures() {
        assertEquals(TransportBoundaryFailureCategory.TIMEOUT, classify(grpc(Status.DEADLINE_EXCEEDED), false));
        assertEquals(TransportBoundaryFailureCategory.AUTH, classify(grpc(Status.UNAUTHENTICATED), false));
        assertEquals(TransportBoundaryFailureCategory.AUTH, classify(grpc(Status.PERMISSION_DENIED), false));
        assertEquals(TransportBoundaryFailureCategory.UNAVAILABLE, classify(grpc(Status.UNAVAILABLE), false));
        assertEquals(TransportBoundaryFailureCategory.PROTOCOL, classify(grpc(Status.INVALID_ARGUMENT), false));
        assertEquals(TransportBoundaryFailureCategory.CANCELLED, classify(grpc(Status.CANCELLED), false));
        assertEquals(TransportBoundaryFailureCategory.UNEXPECTED, classify(grpc(Status.INTERNAL), false));
    }

    @Test
    void classifiesTransitionWorkerFailureMessages() {
        assertEquals(
            TransportBoundaryFailureCategory.TIMEOUT,
            classify(new TransitionWorkerFailureException("Timed out waiting for SQS transition worker response"), false));
        assertEquals(
            TransportBoundaryFailureCategory.MALFORMED,
            classify(new TransitionWorkerFailureException("REST transition worker returned malformed JSON"), false));
        assertEquals(
            TransportBoundaryFailureCategory.PROTOCOL,
            classify(new TransitionWorkerFailureException("gRPC transition worker returned unsupported protocol version"), false));
        assertEquals(
            TransportBoundaryFailureCategory.AUTH,
            classify(new TransitionWorkerFailureException("SQS transition worker signature mismatch"), false));
        assertEquals(
            TransportBoundaryFailureCategory.UNAVAILABLE,
            classify(new TransitionWorkerFailureException("REST transition worker returned HTTP 503"), false));
        assertEquals(
            TransportBoundaryFailureCategory.PROTOCOL,
            classify(new TransitionWorkerFailureException("REST transition worker returned HTTP 400"), false));
        assertEquals(
            TransportBoundaryFailureCategory.UNEXPECTED,
            classify(new TransitionWorkerFailureException("worker returned an unknown failure"), false));
    }

    @Test
    void classifiesNestedAndSuppressedFailures() {
        assertEquals(
            TransportBoundaryFailureCategory.TIMEOUT,
            classify(new CompletionException(new TimeoutException("timed out")), false));

        RuntimeException wrapper = new RuntimeException("outer");
        wrapper.addSuppressed(new SocketTimeoutException("suppressed timeout"));
        assertEquals(TransportBoundaryFailureCategory.TIMEOUT, classify(wrapper, false));
    }

    private TransportBoundaryFailureCategory classify(Throwable failure, boolean cancelled) {
        return classifier.classify(failure, cancelled);
    }

    private WebApplicationException rest(int status) {
        return new WebApplicationException(Response.status(status).build());
    }

    private StatusRuntimeException grpc(Status status) {
        return new StatusRuntimeException(status);
    }
}
