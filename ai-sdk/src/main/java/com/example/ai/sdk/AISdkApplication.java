package com.example.ai.sdk;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class AISdkApplication {
    
    /**
     * Application entry point that starts the Quarkus runtime.
     *
     * @param args command-line arguments forwarded to the Quarkus runtime
     */
    public static void main(String[] args) {
        Quarkus.run(args);
    }
}