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

import java.nio.file.Path;
import java.util.Optional;

/** Runs the independent v3 protobuf and Java-domain generators in one build lifecycle command. */
public final class PipelineV3ContractGenerator {

    private PipelineV3ContractGenerator() {
    }

    public static void main(String[] args) {
        Arguments arguments = Arguments.parse(args);
        if (arguments.help()) {
            System.out.println("Usage: PipelineV3ContractGenerator [--module-dir DIR] [--config PATH] [--output-dir DIR]");
            return;
        }
        generate(arguments.moduleDir(), arguments.configPath(), arguments.outputDir());
    }

    /** Generate both v3 target surfaces while keeping their renderers independent. */
    public static void generate(Path moduleDir, Path configPath, Path outputDir) {
        new PipelineProtoGenerator().generate(moduleDir, configPath, outputDir);
        new PipelineV3JavaDomainGenerator().generate(
            moduleDir, Optional.ofNullable(configPath), Optional.ofNullable(outputDir));
    }

    static record Arguments(Path moduleDir, Path configPath, Path outputDir, boolean help) {
        static Arguments parse(String[] args) {
            Path moduleDir = Path.of("");
            Path configPath = null;
            Path outputDir = null;
            boolean help = false;
            for (int i = 0; i < args.length; i++) {
                String argument = args[i];
                if (argument.startsWith("--module-dir=")) {
                    moduleDir = pathValue("--module-dir", argument.substring("--module-dir=".length()));
                } else if ("--module-dir".equals(argument)) {
                    moduleDir = pathValue("--module-dir", nextValue(args, ++i, "--module-dir"));
                } else if (argument.startsWith("--config=")) {
                    configPath = pathValue("--config", argument.substring("--config=".length()));
                } else if ("--config".equals(argument)) {
                    configPath = pathValue("--config", nextValue(args, ++i, "--config"));
                } else if (argument.startsWith("--output-dir=")) {
                    outputDir = pathValue("--output-dir", argument.substring("--output-dir=".length()));
                } else if ("--output-dir".equals(argument)) {
                    outputDir = pathValue("--output-dir", nextValue(args, ++i, "--output-dir"));
                } else if ("--help".equals(argument) || "-h".equals(argument)) {
                    help = true;
                } else {
                    throw new IllegalArgumentException("Unknown PipelineV3ContractGenerator option: " + argument);
                }
            }
            return new Arguments(moduleDir, configPath, outputDir, help);
        }

        private static String nextValue(String[] args, int index, String option) {
            if (index >= args.length || args[index].isBlank() || args[index].startsWith("--")) {
                throw new IllegalArgumentException(option + " requires a non-blank path value.");
            }
            return args[index];
        }

        private static Path pathValue(String option, String value) {
            if (value.isBlank()) {
                throw new IllegalArgumentException(option + " requires a non-blank path value.");
            }
            return Path.of(value);
        }
    }
}
