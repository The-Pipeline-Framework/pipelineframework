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

package org.pipelineframework.csv.orchestrator;

import java.time.Duration;
import java.util.concurrent.Callable;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import io.quarkus.runtime.QuarkusApplication;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.csv.grpc.InputCsvFileProcessingSvc;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main application class for the orchestrator service.
 * This class provides the proper Quarkus integration for the orchestrator CLI.
 */
@Command(name = "orchestrator", mixinStandardHelpOptions = true, version = "1.0.0",
         description = "CSV Payments Orchestrator Service")
@Dependent
public class OrchestratorApplication implements QuarkusApplication, Callable<Integer> {

    @Option(
        names = {"-i", "--input"},
        description = "Input value for the pipeline",
        defaultValue = "csv"
    )
    public String input;

    @Inject
    PipelineExecutionService pipelineExecutionService;

    @Inject
    PipelineStepConfig pipelineStepConfig;

    public static void main(String[] args) {
        io.quarkus.runtime.Quarkus.run(OrchestratorApplication.class, args);
    }

    @Override
    public int run(String... args) {
        return new CommandLine(this).execute(args);
    }

    /**
     * Orchestrates pipeline execution using a CSV input path from the CLI option or the PIPELINE_INPUT environment variable.
     *
     * If the CLI option `input` is empty, the environment variable `PIPELINE_INPUT` is used. If no input is available, a usage exit code is printed and returned. Otherwise the method constructs the input Multi, invokes the injected pipelineExecutionService to execute the pipeline and waits for completion, then prints a completion message.
     *
     * @return CommandLine.ExitCode.USAGE (non‑zero) if no input was provided, CommandLine.ExitCode.OK on successful execution
     */
    public Integer call() {
        // Use command line option if provided, otherwise fall back to environment variable
        String actualInput = input;
        if (actualInput == null || actualInput.trim().isEmpty()) {
            actualInput = System.getenv("PIPELINE_INPUT");
        }

        if (actualInput == null || actualInput.trim().isEmpty()) {
            System.out.println("Input parameter is empty");
            return CommandLine.ExitCode.USAGE;
        }

        Multi<InputCsvFileProcessingSvc.CsvFolder> inputMulti = getInputMulti(actualInput);

        Duration timeout = pipelineStepConfig.health().startupTimeout();
        pipelineExecutionService.awaitStartupHealth(timeout);


        // Execute the pipeline with the processed input using injected service
        pipelineExecutionService.executePipeline(inputMulti)
                .collect().asList()
                .await().indefinitely();

        System.out.println("Pipeline execution completed");
        return CommandLine.ExitCode.OK;
    }

    private static Multi<InputCsvFileProcessingSvc.CsvFolder> getInputMulti(String input) {
        InputCsvFileProcessingSvc.CsvFolder csvFolder = InputCsvFileProcessingSvc.CsvFolder.newBuilder().setPath(input).build();

        // Create input Multi from the input parameter
        return Multi.createFrom().item(csvFolder);
    }
}
