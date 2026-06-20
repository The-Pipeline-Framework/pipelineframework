package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

class ResolvedCheckpointPublicationTargetTest {

    @Test
    void kafkaTargetRequiresPublishMethod() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready",
                "deliver",
                PublicationTargetKind.KAFKA,
                PublicationEncoding.JSON,
                null,
                null,
                "checkout.orders.ready.v1",
                "POST"));
        assertEquals("KAFKA checkpoint publication targets must use method PUBLISH", error.getMessage());
    }

    @Test
    void kafkaTargetAcceptsPublishMethod() {
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready",
            "deliver",
            PublicationTargetKind.KAFKA,
            PublicationEncoding.JSON,
            null,
            null,
            "checkout.orders.ready.v1",
            "PUBLISH");

        assertEquals(PublicationTargetKind.KAFKA, target.kind());
        assertEquals("PUBLISH", target.method());
        assertEquals("checkout.orders.ready.v1", target.endpoint());
    }

    @Test
    void grpcTargetAcceptsPlaintext() {
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready", "deliver", PublicationTargetKind.GRPC,
            PublicationEncoding.PROTO, null, null, "localhost:9000", "PLAINTEXT");

        assertEquals("PLAINTEXT", target.method());
    }

    @Test
    void grpcTargetAcceptsTls() {
        ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
            "orders-ready", "deliver", PublicationTargetKind.GRPC,
            PublicationEncoding.PROTO, null, null, "downstream:9000", "TLS");

        assertEquals("TLS", target.method());
    }

    @Test
    void grpcTargetRejectsInvalidMethod() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", PublicationTargetKind.GRPC,
                PublicationEncoding.PROTO, null, null, "localhost:9000", "POST"));
        assertEquals("GRPC checkpoint publication targets must use method PLAINTEXT or TLS", error.getMessage());
    }

    @Test
    void httpTargetRequiresPostMethod() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", PublicationTargetKind.HTTP,
                PublicationEncoding.PROTO,
                ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF,
                "Idempotency-Key",
                "http://localhost:8081/path", "PUT"));
        assertEquals("HTTP checkpoint publication targets must use method POST", error.getMessage());
    }

    @Test
    void httpTargetRequiresContentType() {
        assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", PublicationTargetKind.HTTP,
                PublicationEncoding.PROTO,
                null,
                "Idempotency-Key",
                "http://localhost:8081/path", "POST"));
    }

    @Test
    void blankPublicationThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "", "deliver", PublicationTargetKind.KAFKA,
                PublicationEncoding.JSON, null, null,
                "checkout.orders.ready.v1", "PUBLISH"));
    }

    @Test
    void nullKindThrows() {
        assertThrows(NullPointerException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", null,
                PublicationEncoding.JSON, null, null,
                "checkout.orders.ready.v1", "PUBLISH"));
    }

    @Test
    void blankEndpointThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", PublicationTargetKind.KAFKA,
                PublicationEncoding.JSON, null, null,
                "", "PUBLISH"));
    }

    @Test
    void blankMethodThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            new ResolvedCheckpointPublicationTarget(
                "orders-ready", "deliver", PublicationTargetKind.KAFKA,
                PublicationEncoding.JSON, null, null,
                "checkout.orders.ready.v1", ""));
    }
}