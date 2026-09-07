/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.telemetry;

import java.util.ArrayList;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.quarkus.runtime.Startup;

/**
 * Renames Micrometer gRPC metrics to OpenTelemetry RPC semantic conventions.
 */

@ApplicationScoped
public class MetricRenamingConfig {

    /**
     * Default constructor.
     */
    public MetricRenamingConfig() {
    }

    /**
     * Produces a MeterFilter that rewrites gRPC server metric identifiers and tags to RPC-style names.
     *
     * The filter only modifies Meter.Id objects whose name starts with "grpc.server." — other IDs are returned unchanged.
     * When applied, metric names are mapped to RPC equivalents and tags are rewritten (for example:
     * "service" → "rpc.service", "method" → "rpc.method", "statusCode"/"grpc.status" → "rpc.grpc.status_code").
     * If no "rpc.system" tag is present after rewriting, a tag "rpc.system" with value "grpc" is appended.
     *
     * @return a MeterFilter that renames matching gRPC server metric names to RPC names and rewrites/augments their tags; non-matching IDs are left unchanged.
     */
    @Produces
    @Startup
    public MeterFilter grpcRenameFilter() {
        return new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                String name = id.getName();
                if (name == null || !name.startsWith("grpc.server.")) {
                    return id;
                }
                String mappedName = mapGrpcName(name);
                List<Tag> renamedTags = renameGrpcTags(id.getTags());
                return id.withName(mappedName).replaceTags(renamedTags);
            }
        };
    }

    /**
     * Map select gRPC server metric names to their RPC-style equivalents.
     *
     * @param name the original metric name
     * @return {@code rpc.server.duration} for {@code grpc.server.processing.duration},
     *         {@code rpc.server.duration.max} for {@code grpc.server.processing.duration.max},
     *         {@code rpc.server.requests} for {@code grpc.server.requests.received},
     *         {@code rpc.server.responses} for {@code grpc.server.responses.sent},
     *         otherwise returns the original {@code name}
     */
    private static String mapGrpcName(String name) {
        return switch (name) {
            case "grpc.server.processing.duration" -> "rpc.server.duration";
            case "grpc.server.processing.duration.max" -> "rpc.server.duration.max";
            case "grpc.server.requests.received" -> "rpc.server.requests";
            case "grpc.server.responses.sent" -> "rpc.server.responses";
            default -> "rpc.server." + name.substring("grpc.server.".length());
        };
    }

    /**
     * Rename and normalise gRPC-related metric tags, and ensure an `rpc.system` tag is present.
     *
     * <p>Renames keys: `service` → `rpc.service`, `method` → `rpc.method`,
     * `statusCode`/`grpc.status` → `rpc.grpc.status_code`.
     * Preserves existing `rpc.system` tags; if none exists, appends `rpc.system=grpc`.</p>
     *
     * @param tags the original list of metric tags to process
     * @return a new list containing the renamed and possibly augmented tags
     */
    private static List<Tag> renameGrpcTags(List<Tag> tags) {
        List<Tag> renamed = new ArrayList<>(tags.size() + 1);
        boolean hasRpcSystem = false;
        for (Tag tag : tags) {
            String key = tag.getKey();
            String value = tag.getValue();
            if ("service".equals(key)) {
                key = "rpc.service";
            } else if ("method".equals(key)) {
                key = "rpc.method";
            } else if ("grpc.status".equals(key) || "statusCode".equals(key)) {
                key = "rpc.grpc.status_code";
                value = normalizeGrpcStatus(value);
            } else if ("rpc.system".equals(key)) {
                hasRpcSystem = true;
            }
            renamed.add(Tag.of(key, value));
        }
        if (!hasRpcSystem) {
            renamed.add(Tag.of("rpc.system", "grpc"));
        }
        return renamed;
    }

    private static String normalizeGrpcStatus(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        String trimmed = value.trim();
        if (trimmed.chars().allMatch(Character::isDigit)) {
            return trimmed;
        }
        return switch (trimmed) {
            case "OK" -> "0";
            case "CANCELLED" -> "1";
            case "UNKNOWN" -> "2";
            case "INVALID_ARGUMENT" -> "3";
            case "DEADLINE_EXCEEDED" -> "4";
            case "NOT_FOUND" -> "5";
            case "ALREADY_EXISTS" -> "6";
            case "PERMISSION_DENIED" -> "7";
            case "RESOURCE_EXHAUSTED" -> "8";
            case "FAILED_PRECONDITION" -> "9";
            case "ABORTED" -> "10";
            case "OUT_OF_RANGE" -> "11";
            case "UNIMPLEMENTED" -> "12";
            case "INTERNAL" -> "13";
            case "UNAVAILABLE" -> "14";
            case "DATA_LOSS" -> "15";
            case "UNAUTHENTICATED" -> "16";
            default -> trimmed;
        };
    }
}
