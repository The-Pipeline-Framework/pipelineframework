package org.pipelineframework.processor.phase;

import java.util.Optional;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.ir.TransportMode;

/**
 * Resolves transport and platform modes from configuration values.
 * Consolidates the duplicated parse-with-default-and-warning pattern.
 */
class TransportPlatformResolver {

    /**
     * Resolve the transport mode from a configuration value.
     *
     * @param value the raw transport string, may be null or blank
     * @param messager the messager for warning reporting, may be null
     * @return the resolved transport mode, defaults to GRPC
     */
    TransportMode resolveTransport(String value, Messager messager) {
        if (value == null || value.isBlank()) {
            return TransportMode.GRPC;
        }
        Optional<TransportMode> mode = TransportMode.fromStringOptional(value);
        if (mode.isEmpty()) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.WARNING,
                    "Unknown pipeline transport '" + value + "'; defaulting to GRPC.");
            }
            return TransportMode.GRPC;
        }
        return mode.get();
    }

    /**
     * Resolve the platform mode from a configuration value.
     *
     * @param value the raw platform string, may be null or blank
     * @param messager the messager for warning reporting, may be null
     * @return the resolved platform mode, defaults to COMPUTE
     */
    PlatformMode resolvePlatform(String value, Messager messager) {
        if (value == null || value.isBlank()) {
            return PlatformMode.COMPUTE;
        }
        Optional<PlatformMode> mode = PlatformMode.fromStringOptional(value);
        if (mode.isEmpty()) {
            if (messager != null) {
                messager.printMessage(Diagnostic.Kind.WARNING,
                    "Unknown pipeline platform '" + value + "'; defaulting to COMPUTE.");
            }
            return PlatformMode.COMPUTE;
        }
        return mode.get();
    }
}
