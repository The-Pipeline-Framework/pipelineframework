package org.pipelineframework.csv.orchestrator;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;

public class OrchestratorServiceApplication implements QuarkusApplication {

    public static void main(String... args) {
        Quarkus.run(OrchestratorServiceApplication.class, args);
    }

    @Override
    public int run(String... args) {
        Quarkus.waitForExit();
        return 0;
    }
}
