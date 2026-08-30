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

package org.pipelineframework.extension;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ConfigMappingBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import org.pipelineframework.command.CommandEffectStoreConfig;
import org.pipelineframework.query.QueryCaptureStoreConfig;

/**
 * The main Quarkus extension processor for the Pipeline Framework.
 */
public class PipelineFrameworkProcessor {

    private static final String FEATURE_NAME = "pipelineframework";

    /**
     * Default constructor for PipelineFrameworkProcessor.
     */
    public PipelineFrameworkProcessor() {
    }

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE_NAME);
    }

    @BuildStep
    IndexDependencyBuildItem indexDependency() {
        return new IndexDependencyBuildItem("org.pipelineframework", "pipelineframework");
    }

    @BuildStep
    ConfigMappingBuildItem queryCaptureStoreConfig() {
        return new ConfigMappingBuildItem(QueryCaptureStoreConfig.class, "pipeline.query.capture-store");
    }

    @BuildStep
    ConfigMappingBuildItem commandEffectStoreConfig() {
        return new ConfigMappingBuildItem(CommandEffectStoreConfig.class, "pipeline.command.effect-store");
    }
}
