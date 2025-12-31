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

package org.pipelineframework.processor.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.DescriptorProtos;

/**
 * Utility class to locate and load protobuf FileDescriptorSet files during annotation processing.
 * This class locates descriptor files via explicit annotation processor options or
 * well-known Quarkus/Maven output paths, with sensible defaults for multi-module builds.
 *
 * <p>Supported annotation processor options (highest priority):</p>
 * <ul>
 *   <li><code>-Aprotobuf.descriptor.file=&lt;path&gt;</code> for a specific descriptor file</li>
 *   <li><code>-Aprotobuf.descriptor.path=&lt;path&gt;</code> for a directory containing the descriptor file</li>
 * </ul>
 *
 * <p>Default search behavior (when options are not provided):</p>
 * <ul>
 *   <li>Look for <code>target/generated-sources/grpc/descriptor_set.dsc</code> in the current module</li>
 *   <li>Look for the same path in a sibling <code>common</code> module</li>
 *   <li>Scan sibling modules for the same path under a multi-module root</li>
 * </ul>
 */
public class DescriptorFileLocator {

    /**
     * Key for annotation processor option specifying the descriptor file path.
     */
    public static final String DESCRIPTOR_PATH_OPTION = "protobuf.descriptor.path";

    /**
     * Key for annotation processor option specifying a specific descriptor file.
     */
    public static final String DESCRIPTOR_FILE_OPTION = "protobuf.descriptor.file";

    /**
     * Default module name that holds protobuf definitions in multi-module builds.
     */
    private static final String DEFAULT_DESCRIPTOR_MODULE = "common";

    /**
     * Default descriptor file path relative to a module directory.
     */
    private static final String DEFAULT_DESCRIPTOR_RELATIVE_PATH = "target/generated-sources/grpc/descriptor_set.dsc";

    /**
     * Common default locations where protobuf descriptor files are located in Quarkus/Maven builds.
     */
    private static final String[] DEFAULT_LOCATIONS = {
            "target/generated-sources/grpc",                           // Quarkus default
            "target/generated-resources/protobuf/descriptor-set-out"   // Maven default
    };

    /**
     * Common filename for protobuf descriptor files.
     */
    private static final String DESCRIPTOR_FILE_NAME = "descriptor_set.dsc";

    /**
     * Supported descriptor file names when searching directories.
     */
    private static final String[] DESCRIPTOR_FILE_NAMES = {
            DESCRIPTOR_FILE_NAME,
            "descriptor_set.pb",
            "descriptor_set.proto.bin"
    };

    /**
     * Max depth for scanning modules for the default descriptor path.
     */
    private static final int DEFAULT_SEARCH_DEPTH = 4;

    /**
     * Locate and load a protobuf FileDescriptorSet following explicit options or sensible defaults.
     *
     * <p>The method first honours the processor option for a specific descriptor file, then a
     * processor option pointing to a directory, and finally searches sensible module and sibling
     * locations (including the default relative path) until a readable descriptor is found.</p>
     *
     * @param processorOptions annotation processor options that may contain descriptor location hints
     * @return the loaded and parsed FileDescriptorSet
     * @throws IOException if no readable descriptor file can be found or if the found file cannot be read
     */
    public DescriptorProtos.FileDescriptorSet locateAndLoadDescriptors(Map<String, String> processorOptions) throws IOException {
        // First, check if a specific descriptor file is provided via annotation processor option
        String specificFile = processorOptions.get(DESCRIPTOR_FILE_OPTION);
        if (specificFile != null && !specificFile.trim().isEmpty()) {
            Path specificPath = Paths.get(specificFile);
            if (Files.exists(specificPath) && Files.isReadable(specificPath)) {
                return loadDescriptorFile(specificPath);
            } else {
                throw new IOException("Specified descriptor file does not exist or is not readable: " + specificFile);
            }
        }

        // Second, check if a descriptor path is provided via annotation processor option
        String descriptorPath = processorOptions.get(DESCRIPTOR_PATH_OPTION);
        if (descriptorPath != null && !descriptorPath.trim().isEmpty()) {
            Path basePath = Paths.get(descriptorPath);
            if (Files.exists(basePath) && Files.isRegularFile(basePath) && Files.isReadable(basePath)) {
                return loadDescriptorFile(basePath);
            }
            if (Files.exists(basePath) && Files.isDirectory(basePath)) {
                Optional<Path> descriptorFile = findDescriptorFile(basePath);
                if (descriptorFile.isPresent()) {
                    return loadDescriptorFile(descriptorFile.get());
                }
                throw new IOException(
                    "No descriptor file found in " + descriptorPath +
                        ". Expected one of: " + String.join(", ", DESCRIPTOR_FILE_NAMES));
            }
            throw new IOException("Specified descriptor path does not exist or is not readable: " + descriptorPath);
        }

        // Third, check smart defaults based on module roots
        DescriptorProtos.FileDescriptorSet descriptorSet;
        for (Path baseDir : getBaseDirectories()) {
            descriptorSet = locateFromBase(baseDir);
            if (descriptorSet != null) {
                return descriptorSet;
            }
        }

        throw new IOException(
            "No protobuf descriptor file found. Configure the compiler with " +
                "-A" + DESCRIPTOR_FILE_OPTION + "=<path-to-descriptor-set> " +
                "or -A" + DESCRIPTOR_PATH_OPTION + "=<descriptor-directory>. " +
                "Defaults searched: " + DEFAULT_DESCRIPTOR_RELATIVE_PATH + " in module, " +
                "module '" + DEFAULT_DESCRIPTOR_MODULE + "', and sibling modules.");
    }

    /**
         * Locate a protobuf descriptor file by checking known descriptor filenames in the specified directory.
         *
         * @param directory the directory to search
         * @return an {@link Optional} containing the readable descriptor file path if found, otherwise an empty {@link Optional}
         */
    private Optional<Path> findDescriptorFile(Path directory) {
        for (String fileName : DESCRIPTOR_FILE_NAMES) {
            Path candidate = directory.resolve(fileName);
            if (Files.exists(candidate) && Files.isReadable(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /**
     * Attempt to locate a protobuf FileDescriptorSet starting from a base directory using multiple search strategies.
     *
     * <p>Checks the given directory and common module locations (including a "common" module and sibling modules),
     * then performs a depth-limited directory walk; returns the first matching descriptor found.</p>
     *
     * @param baseDirectory the directory to start searching from; may be null
     * @return the parsed FileDescriptorSet if found, `null` if no descriptor was located
     * @throws IOException if an I/O error occurs while traversing directories or reading descriptor files
     */
    private DescriptorProtos.FileDescriptorSet locateFromBase(Path baseDirectory) throws IOException {
        if (baseDirectory == null) {
            return null;
        }

        Path baseDir = baseDirectory.toAbsolutePath();
        DescriptorProtos.FileDescriptorSet descriptorSet = loadFromModulePath(baseDir);
        if (descriptorSet != null) {
            return descriptorSet;
        }

        descriptorSet = loadFromModulePath(baseDir.resolve(DEFAULT_DESCRIPTOR_MODULE));
        if (descriptorSet != null) {
            return descriptorSet;
        }

        Path parentDir = baseDir.getParent();
        if (parentDir != null) {
            descriptorSet = loadFromModulePath(parentDir.resolve(DEFAULT_DESCRIPTOR_MODULE));
            if (descriptorSet != null) {
                return descriptorSet;
            }

            try (var siblings = Files.list(parentDir)) {
                for (Path sibling : (Iterable<Path>) siblings.filter(Files::isDirectory)::iterator) {
                    descriptorSet = loadFromModulePath(sibling);
                    if (descriptorSet != null) {
                        return descriptorSet;
                    }

                    descriptorSet = loadFromModulePath(sibling.resolve(DEFAULT_DESCRIPTOR_MODULE));
                    if (descriptorSet != null) {
                        return descriptorSet;
                    }
                }
            }
        }

        descriptorSet = findDescriptorByWalk(baseDir, DEFAULT_SEARCH_DEPTH);
        if (descriptorSet != null) {
            return descriptorSet;
        }

        if (parentDir != null) {
            descriptorSet = findDescriptorByWalk(parentDir, DEFAULT_SEARCH_DEPTH);
            return descriptorSet;
        }

        return null;
    }

    /**
     * Attempt to load a protobuf FileDescriptorSet from common locations under the given module directory.
     *
     * Searches the module for the default descriptor relative path and for known alternate locations, and parses
     * the first readable descriptor file found.
     *
     * @param moduleDirectory the module root directory to search; may be null
     * @return the parsed FileDescriptorSet if a descriptor file is found and readable, `null` otherwise
     * @throws IOException if a discovered descriptor file cannot be read or parsed
     */
    private DescriptorProtos.FileDescriptorSet loadFromModulePath(Path moduleDirectory) throws IOException {
        if (moduleDirectory == null || !Files.exists(moduleDirectory) || !Files.isDirectory(moduleDirectory)) {
            return null;
        }

        Path defaultDescriptorPath = moduleDirectory.resolve(DEFAULT_DESCRIPTOR_RELATIVE_PATH);
        if (Files.exists(defaultDescriptorPath) && Files.isReadable(defaultDescriptorPath)) {
            return loadDescriptorFile(defaultDescriptorPath);
        }

        for (String defaultLocation : DEFAULT_LOCATIONS) {
            Path locationPath = moduleDirectory.resolve(defaultLocation);
            if (Files.exists(locationPath) && Files.isDirectory(locationPath)) {
                Optional<Path> descriptorFile = findDescriptorFile(locationPath);
                if (descriptorFile.isPresent()) {
                    return loadDescriptorFile(descriptorFile.get());
                }
            }
        }

        return null;
    }

    /**
     * Search a directory tree up to a specified depth for a protobuf descriptor file at the default relative path and load it.
     *
     * Searches every directory under `baseDir` (including `baseDir`) up to `maxDepth` for a file at
     * DEFAULT_DESCRIPTOR_RELATIVE_PATH; when a readable file is found it is parsed and returned.
     *
     * @param baseDir  the root directory to start the walk from; ignored if null or not a directory
     * @param maxDepth the maximum directory depth to traverse from `baseDir`
     * @return the parsed {@code DescriptorProtos.FileDescriptorSet} if a descriptor file is found, or {@code null} if none is found or an I/O error occurs while walking
     * @throws IOException if an I/O error occurs while reading or parsing a discovered descriptor file
     */
    private DescriptorProtos.FileDescriptorSet findDescriptorByWalk(Path baseDir, int maxDepth) throws IOException {
        if (baseDir == null || !Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
            return null;
        }

        try (var paths = Files.walk(baseDir, maxDepth)) {
            for (Path dir : (Iterable<Path>) paths.filter(Files::isDirectory)::iterator) {
                Path candidate = dir.resolve(DEFAULT_DESCRIPTOR_RELATIVE_PATH);
                if (Files.exists(candidate) && Files.isReadable(candidate)) {
                    return loadDescriptorFile(candidate);
                }
            }
        } catch (UncheckedIOException e) {
            return null;
        }

        return null;
    }

    /**
     * Compute base directories used as roots when searching for descriptor files.
     *
     * @return a LinkedHashSet of Paths containing the Maven `maven.multiModuleProjectDirectory` (if set and not blank)
     *         followed by the current working directory from `user.dir` (defaults to ".")
     */
    private java.util.Set<Path> getBaseDirectories() {
        java.util.Set<Path> baseDirs = new java.util.LinkedHashSet<>();
        String multiModuleDir = System.getProperty("maven.multiModuleProjectDirectory");
        if (multiModuleDir != null && !multiModuleDir.isBlank()) {
            baseDirs.add(Paths.get(multiModuleDir));
        }
        baseDirs.add(Paths.get(System.getProperty("user.dir", ".")));
        return baseDirs;
    }

    /**
     * Load a protobuf descriptor set from the specified file path.
     *
     * @param filePath the path to the descriptor file; must point to a readable file
     * @return the parsed FileDescriptorSet
     * @throws IOException if the file cannot be read or the descriptor cannot be parsed
     */
    private DescriptorProtos.FileDescriptorSet loadDescriptorFile(Path filePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            return DescriptorProtos.FileDescriptorSet.parseFrom(fis);
        } catch (IOException e) {
            throw new IOException("Failed to parse FileDescriptorSet from " + filePath + ": " + e.getMessage(), e);
        }
    }
}