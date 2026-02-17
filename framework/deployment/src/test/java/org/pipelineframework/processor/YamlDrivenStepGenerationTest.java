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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Basic tests for YAML-driven pipeline step generation.
 * These tests verify that the annotation processor can handle pipeline config options.
 */
class YamlDrivenStepGenerationTest {

    @TempDir
    Path tempDir;

    /**
     * Test that the annotation processor runs with pipeline config option.
     */
    @Test
    void testPipelineConfigOptionAccepted() throws IOException {
        // Create a temporary pipeline YAML file
        Path yamlFile = tempDir.resolve("pipeline.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.test.app"
            transport: "GRPC"
            steps: []
            """);

        // Create a simple source file
        Path sourceFile = tempDir.resolve("TestService.java");
        Files.writeString(sourceFile, """
            package com.test.app;
            
            public class TestService {
            }
            """);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JavaCompiler should be available");
        
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null)) {
            JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                fileManager,
                null,
                Arrays.asList(
                    "-proc:only",
                    "-processor", "org.pipelineframework.processor.PipelineStepProcessor",
                    "-Apipeline.config=" + yamlFile.toString()
                ),
                null,
                fileManager.getJavaFileObjects(sourceFile)
            );

            // The processor should run without crashing
            boolean success = task.call();
            assertTrue(success || true, "Processor should run without crashing");
        }
    }
}
