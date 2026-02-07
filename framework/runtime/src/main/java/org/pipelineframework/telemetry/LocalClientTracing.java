package org.pipelineframework.telemetry;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Adds client-side telemetry around in-process calls.
 */
public final class LocalClientTracing {

    private LocalClientTracing() {
    }

    // TODO: switch to local-specific metrics/spans once dashboards can handle rpc.system=local.
    public static <T> Uni<T> traceUnary(String service, String method, Uni<T> uni) {
        return GrpcClientTracing.traceUnary(service, method, uni);
    }

    public static <T> Multi<T> traceMulti(String service, String method, Multi<T> multi) {
        return GrpcClientTracing.traceMulti(service, method, multi);
    }
}
