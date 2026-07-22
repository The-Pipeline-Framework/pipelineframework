/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.proto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.template.*;

/**
 * Generates the Java target for a resolved version 3 template.
 *
 * <p>This is deliberately independent of {@link PipelineProtoGenerator}. A build may invoke the
 * proto and Java generators in the same {@code generate-sources} lifecycle, but neither target
 * renderer owns the other.</p>
 */
public final class PipelineV3JavaDomainGenerator {
    private static final ObjectMapper IDL_MAPPER = PipelineJson.mapper().copy().findAndRegisterModules();
    private final PipelineV3GenerationCoordinator coordinator = new PipelineV3GenerationCoordinator();

    /** Command-line entry point used by build integrations. */
    public static void main(String[] args) {
        Arguments arguments = Arguments.parse(args);
        new PipelineV3JavaDomainGenerator().generate(
            arguments.moduleDir().orElse(Path.of("")), arguments.configPath(), arguments.outputDir());
    }

    /** Generate domain records, wrappers, unions, and adapters from a committed v3 IDL state. */
    public void generate(Path moduleDir, Path configPath, Path outputDir) {
        generate(moduleDir, Optional.ofNullable(configPath), Optional.ofNullable(outputDir));
    }

    /** Generate domain records, wrappers, unions, and adapters from explicit optional build arguments. */
    public void generate(Path moduleDir, Optional<Path> configPath, Optional<Path> outputDir) {
        Path resolvedModuleDir = java.util.Objects.requireNonNull(moduleDir, "moduleDir must not be null");
        Path resolvedConfig = resolveConfigPath(resolvedModuleDir, configPath).toAbsolutePath().normalize();
        Path resolvedOutput = outputDir.orElseGet(() ->
            resolvedModuleDir.resolve("target").resolve("generated-sources").resolve("pipeline-domain"));
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(resolvedConfig);
        if (config.dialect() != PipelineTemplateDialect.V3) {
            throw new IllegalStateException("Java domain generation requires version: 3.");
        }
        Path statePath = resolveIdlStatePath(resolvedConfig);
        if (!Files.exists(statePath)) {
            throw new IllegalStateException("Version 3 Java domain generation requires committed IDL state: " + statePath);
        }
        PipelineIdlSnapshot baseline = readState(statePath);
        PipelineIdlSnapshot state = new PipelineIdlStateResolver().resolve(config, baseline, false).state();
        coordinator.generateJava(resolvedOutput, coordinator.plan(config, state));
    }

    private Path resolveConfigPath(Path moduleDir, Optional<Path> explicit) {
        return explicit.orElseGet(() -> new PipelineYamlConfigLocator().locate(moduleDir)
            .orElseThrow(() -> new IllegalStateException("Pipeline template config not found under " + moduleDir)));
    }

    private PipelineIdlSnapshot readState(Path statePath) {
        try {
            return IDL_MAPPER.readValue(statePath.toFile(), PipelineIdlSnapshot.class);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read committed IDL state: " + statePath, e);
        }
    }

    private Path resolveIdlStatePath(Path configPath) {
        String configFileName = configPath.getFileName().toString();
        String stateFileName = configFileName.endsWith(".yaml")
            ? configFileName.substring(0, configFileName.length() - ".yaml".length()) + ".idl.json"
            : "pipeline.idl.json";
        return configPath.getParent().resolve(stateFileName);
    }

    private record Arguments(Optional<Path> moduleDir, Optional<Path> configPath, Optional<Path> outputDir) {
        private static Arguments parse(String[] args) {
            Optional<Path> moduleDir = Optional.empty();
            Optional<Path> configPath = Optional.empty();
            Optional<Path> outputDir = Optional.empty();
            for (String argument : args) {
                if (argument == null || argument.isBlank()) {
                    continue;
                }
                if (argument.startsWith("--module-dir=")) {
                    moduleDir = Optional.of(Path.of(argument.substring("--module-dir=".length())));
                } else if (argument.startsWith("--config=")) {
                    configPath = Optional.of(Path.of(argument.substring("--config=".length())));
                } else if (argument.startsWith("--output-dir=")) {
                    outputDir = Optional.of(Path.of(argument.substring("--output-dir=".length())));
                } else {
                    throw new IllegalArgumentException("Unsupported version 3 Java domain generator argument: " + argument);
                }
            }
            return new Arguments(moduleDir, configPath, outputDir);
        }
    }
}
