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

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineOrchestrator;

import static org.pipelineframework.processor.PipelineStepProcessor.GRPC_SERVICE_SUFFIX;

/**
 * Registers generated gRPC service classes as additional unremovable beans when CLI generation is disabled.
 */
public class StepServerRegistrar {

    private static final String FEATURE_NAME = "pipelineframework-services";
    private static final Logger LOG = Logger.getLogger(StepServerRegistrar.class);

    /**
     * Default constructor for StepServerRegistrar.
     */
    public StepServerRegistrar() {
    }

    /**
     * Declare the extension feature provided by this processor.
     *
     * @return a FeatureBuildItem identifying the processor feature named "pipelineframework-services"
     */
    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE_NAME);
    }

    /**
     * Registers generated gRPC service classes as unremovable CDI beans when CLI generation is disabled.
     * <p>
     * If CLI generation is enabled this step performs no registration.
     *
     * @param additionalBeans producer used to register discovered service classes as unremovable CDI beans
     * @param config build-time configuration that controls whether CLI generation is enabled
     * @param combinedIndex provides the index used to discover generated gRPC service classes
     */
    @BuildStep
    void registerGeneratedGrpcServices(BuildProducer<AdditionalBeanBuildItem> additionalBeans,
                                  CombinedIndexBuildItem combinedIndex) {

        if (isCliGenerationEnabled(combinedIndex)) {
            LOG.debug("Client generation enabled; skipping server registration.");
            return;
        }

        IndexView index = combinedIndex.getIndex();

        // Find all classes ending with "GrpcService" - these need explicit registration
        index.getKnownClasses().stream()
            .filter(ci -> ci.name().toString().endsWith(GRPC_SERVICE_SUFFIX))
            .forEach(ci -> {
                LOG.infof("Registering gRPC service: %s", ci.name());
                additionalBeans.produce(AdditionalBeanBuildItem.unremovableOf(ci.name().toString()));
            });
    }

    private boolean isCliGenerationEnabled(CombinedIndexBuildItem combinedIndex) {
        DotName annotationName = DotName.createSimple(PipelineOrchestrator.class.getName());
        java.util.Collection<AnnotationInstance> instances = combinedIndex.getIndex().getAnnotations(annotationName);
        if (instances == null || instances.isEmpty()) {
            return false;
        }
        for (AnnotationInstance instance : instances) {
            AnnotationValue value = instance.value("generateCli");
            if (value == null || value.asBoolean()) {
                return true;
            }
        }
        return false;
    }
}
