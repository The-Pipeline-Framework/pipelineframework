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

package org.pipelineframework.processor;

import java.util.List;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineCliAppConfig;

import static org.pipelineframework.processor.PipelineStepProcessor.CLIENT_STEP_SUFFIX;

/**
 * Registers client step classes as additional unremovable beans when CLI client generation is enabled.
 */
public class StepClientRegistrar {

    private static final String FEATURE_NAME = "pipelineframework-steps";
    private static final Logger LOG = Logger.getLogger(StepClientRegistrar.class);

    /**
     * Default constructor for StepClientRegistrar.
     */
    public StepClientRegistrar() {
    }

    /**
     * Declares the build feature provided by this extension.
     *
     * @return the FeatureBuildItem for the "pipelineframework-steps" feature
     */
    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE_NAME);
    }

    /**
     * Exposes discovered pipeline step client classes as additional unremovable beans when CLI client generation is enabled.
     * <p>
     * Scans the provided Jandex index for classes whose simple name ends with the configured client step suffix and registers each
     * matching class as an unremovable AdditionalBeanBuildItem when CLI generation is enabled via the supplied configuration.
     *
     * @param config controls whether CLI-generated clients should be registered
     * @param combinedIndex combined Jandex index containing application classes to scan
     */
    @BuildStep
    void registerStepClients(BuildProducer<AdditionalBeanBuildItem> beans,
                             PipelineCliAppConfig config,
                             CombinedIndexBuildItem combinedIndex) {
        if (!config.generateCli()) {
            LOG.debug("Client generation disabled; skipping client step registration.");
            return;
        }

        IndexView index = combinedIndex.getIndex();

        // Find all classes ending with "ClientStep"
        List<ClassInfo> classes = index.getKnownClasses().stream()
                .filter(ci -> ci.name().toString().endsWith(CLIENT_STEP_SUFFIX))
                .toList();

        for (ClassInfo ci : classes) {
            beans.produce(AdditionalBeanBuildItem.unremovableOf(ci.name().toString()));
            LOG.infof("Registered step (client) %s", ci.name());
        }
    }
}