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

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Objects;
import javax.annotation.processing.Filer;
import javax.tools.StandardLocation;

import org.pipelineframework.processor.ir.PipelineStepModel;

/**
 * Writes step definition metadata to a file that can be read by the Quarkus build step.
 * <p>
 * The annotation processor runs during javac and cannot directly produce Quarkus build items.
 * Instead, it writes step metadata to {@code META-INF/pipeline/step-definitions.txt} which
 * the Quarkus build step reads during the augmentation phase.
 */
public class StepDefinitionWriter {

    private static final String STEP_DEFINITIONS_FILE = "META-INF/pipeline/step-definitions.txt";

    private final Filer filer;

    /**
     * Creates a new step definition writer.
     *
     * @param filer the filer for writing resources
     */
    public StepDefinitionWriter(Filer filer) {
        this.filer = Objects.requireNonNull(filer,
                "StepDefinitionWriter requires a non-null Filer; write() cannot run without it");
    }

    /**
     * Writes step definitions to the metadata file.
     * <p>
     * Format (one line per step):
     * <pre>
     * stepClassName|domainInFQN|domainOutFQN|cardinality
     * </pre>
     *
     * @param stepModels the step models to write
     * @throws IOException if writing fails
     */
    public void write(List<PipelineStepModel> stepModels) throws IOException {
        if (stepModels.isEmpty()) {
            return;
        }

        try (Writer writer = createResourceWriter()) {
            for (PipelineStepModel step : stepModels) {
                String domainIn = step.inputMapping() != null && step.inputMapping().domainType() != null
                        ? step.inputMapping().domainType().toString()
                        : "";
                String domainOut = step.outputMapping() != null && step.outputMapping().domainType() != null
                        ? step.outputMapping().domainType().toString()
                        : "";

                // Get cardinality from streaming shape
                String cardinality = step.streamingShape() != null ? step.streamingShape().name() : "ONE_TO_ONE";
                String serviceName = step.serviceName();
                // The metadata format is pipe-delimited; service names must not include '|'.
                if (serviceName != null && serviceName.contains("|")) {
                    throw new IllegalArgumentException(
                            "Step serviceName contains unsupported delimiter '|': " + serviceName);
                }

                String line = String.format("%s|%s|%s|%s%n",
                        serviceName,
                        domainIn,
                        domainOut,
                        cardinality);

                writer.write(line);
            }
        }
    }

    /**
     * Obtain a Writer for the step definitions resource.
     *
     * Attempts to create the resource in CLASS_OUTPUT and, if that fails, in SOURCE_OUTPUT;
     * if both attempts fail the exception from the CLASS_OUTPUT attempt is added as suppressed to the thrown exception.
     *
     * @return a Writer for META-INF/pipeline/step-definitions.txt
     * @throws IOException if creating the resource fails in both CLASS_OUTPUT and SOURCE_OUTPUT
     */
    private Writer createResourceWriter() throws IOException {
        // Try to create the resource in CLASS_OUTPUT (target/classes)
        try {
            return filer.createResource(StandardLocation.CLASS_OUTPUT, "", STEP_DEFINITIONS_FILE)
                    .openWriter();
        } catch (IOException e1) {
            // If CLASS_OUTPUT fails, try SOURCE_OUTPUT
            try {
                return filer.createResource(StandardLocation.SOURCE_OUTPUT, "", STEP_DEFINITIONS_FILE)
                        .openWriter();
            } catch (IOException e2) {
                e2.addSuppressed(e1);
                throw e2;
            }
        }
    }
}