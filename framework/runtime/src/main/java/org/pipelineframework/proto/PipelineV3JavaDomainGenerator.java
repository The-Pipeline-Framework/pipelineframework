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

    /** Generate domain records, wrappers, unions, and adapters from a committed v3 IDL state. */
    public void generate(Path moduleDir, Path configPath, Path outputDir) {
        Path resolvedModuleDir = moduleDir == null ? Path.of("") : moduleDir;
        Path resolvedConfig = resolveConfigPath(resolvedModuleDir, configPath).toAbsolutePath().normalize();
        Path resolvedOutput = outputDir != null
            ? outputDir : resolvedModuleDir.resolve("target").resolve("generated-sources").resolve("pipeline-domain");
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

    private Path resolveConfigPath(Path moduleDir, Path explicit) {
        if (explicit != null) {
            return explicit;
        }
        return new PipelineYamlConfigLocator().locate(moduleDir)
            .orElseThrow(() -> new IllegalStateException("Pipeline template config not found under " + moduleDir));
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
}
