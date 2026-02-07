package org.pipelineframework.telemetry;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Adds client-side telemetry around in-process calls.
 */
public final class LocalClientTracing {

    /**
     * Prevents instantiation of this utility class.
     */
    private LocalClientTracing() {
    }

    /**
     * Adds client-side tracing to a unary in-process call.
     *
     * @param service the service name to associate with the traced call
     * @param method  the method name to associate with the traced call
     * @param uni     the Uni representing the unary operation to be traced
     * @param <T>     the type emitted by the Uni
     * @return        a Uni that emits the same items as the provided `uni` with client-side tracing applied
     */
    public static <T> Uni<T> traceUnary(String service, String method, Uni<T> uni) {
        return GrpcClientTracing.traceUnary(service, method, uni);
    }

    /**
     * Adds client-side telemetry to a streaming (Multi) call identified by the given service and method.
     *
     * @param service the RPC service name
     * @param method the RPC method name
     * @param multi the original reactive stream to instrument
     * @param <T> the type of elements emitted by the stream
     * @return the same stream augmented with client-side telemetry instrumentation
     */
    public static <T> Multi<T> traceMulti(String service, String method, Multi<T> multi) {
        return GrpcClientTracing.traceMulti(service, method, multi);
    }
}