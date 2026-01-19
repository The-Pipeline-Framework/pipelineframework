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
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

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
     * Creates a new DescriptorFileLocator.
     */
    public DescriptorFileLocator() {
    }

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
        return locateAndLoadDescriptors(processorOptions, null, null);
    }

    /**
     * Locate and load a protobuf FileDescriptorSet, preferring descriptor sets that contain the expected services.
     *
     * <p>If expected service names are provided and no explicit descriptor options are set, the locator scans
     * candidate descriptor sets and returns the first one that declares at least one of the expected service names.</p>
     *
     * @param processorOptions annotation processor options that may contain descriptor location hints
     * @param expectedServiceNames service names that should be present in the descriptor set (may be null or empty)
     * @return the loaded and parsed FileDescriptorSet
     * @throws IOException if no readable descriptor file can be found or if the found file cannot be read
     */
    public DescriptorProtos.FileDescriptorSet locateAndLoadDescriptors(
            Map<String, String> processorOptions,
            java.util.Set<String> expectedServiceNames) throws IOException {
        return locateAndLoadDescriptors(processorOptions, expectedServiceNames, null);
    }

    /**
     * Locate and load a protobuf FileDescriptorSet, optionally emitting selection details via the annotation processor messager.
     *
     * @param processorOptions annotation processor options that may contain descriptor location hints
     * @param expectedServiceNames service names that should be present in the descriptor set (may be null or empty)
     * @param messager optional messager for emitting selection info
     * @return the loaded and parsed FileDescriptorSet
     * @throws IOException if no readable descriptor file can be found or if the found file cannot be read
     */
    public DescriptorProtos.FileDescriptorSet locateAndLoadDescriptors(
            Map<String, String> processorOptions,
            java.util.Set<String> expectedServiceNames,
            Messager messager) throws IOException {
        // First, check if a specific descriptor file is provided via annotation processor option
        String specificFile = processorOptions.get(DESCRIPTOR_FILE_OPTION);
        if (specificFile != null && !specificFile.trim().isEmpty()) {
            Path specificPath = Paths.get(specificFile);
            if (Files.exists(specificPath) && Files.isReadable(specificPath)) {
                logSelectedDescriptor(messager, specificPath);
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
                logSelectedDescriptor(messager, basePath);
                return loadDescriptorFile(basePath);
            }
            if (Files.exists(basePath) && Files.isDirectory(basePath)) {
                Optional<Path> descriptorFile = findDescriptorFile(basePath);
                if (descriptorFile.isPresent()) {
                    logSelectedDescriptor(messager, descriptorFile.get());
                    return loadDescriptorFile(descriptorFile.get());
                }
                throw new IOException(
                    "No descriptor file found in " + descriptorPath +
                        ". Expected one of: " + String.join(", ", DESCRIPTOR_FILE_NAMES));
            }
            throw new IOException("Specified descriptor path does not exist or is not readable: " + descriptorPath);
        }

        // Third, prefer module-local descriptor sets when no expected services are provided
        if (expectedServiceNames == null || expectedServiceNames.isEmpty()) {
            java.util.List<Path> preferredCandidates = new java.util.ArrayList<>();
            Path moduleDir = Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath();
            addCandidatesFromModulePath(moduleDir, preferredCandidates);
            Path moduleParent = moduleDir.getParent();
            if (moduleParent != null) {
                addCandidatesFromModulePath(moduleParent.resolve(DEFAULT_DESCRIPTOR_MODULE), preferredCandidates);
            }
            preferredCandidates = dedupeCandidates(preferredCandidates);
            for (Path candidate : preferredCandidates) {
                DescriptorProtos.FileDescriptorSet descriptorSet = loadDescriptorFile(candidate);
                logSelectedDescriptor(messager, candidate);
                return descriptorSet;
            }
        }

        // Fourth, check smart defaults based on module roots
        java.util.List<Path> candidates = new java.util.ArrayList<>();
        for (Path baseDir : getBaseDirectories()) {
            candidates.addAll(collectDescriptorCandidates(baseDir));
        }
        candidates = dedupeCandidates(candidates);

        for (Path candidate : candidates) {
            DescriptorProtos.FileDescriptorSet descriptorSet = loadDescriptorFile(candidate);
            if (expectedServiceNames == null || expectedServiceNames.isEmpty()) {
                logSelectedDescriptor(messager, candidate);
                return descriptorSet;
            }
            if (containsAnyService(descriptorSet, expectedServiceNames)) {
                logSelectedDescriptor(messager, candidate);
                return descriptorSet;
            }
        }

        if (!candidates.isEmpty() && !expectedServiceNames.isEmpty()) {
            throw new IOException(
                "No protobuf descriptor file found containing expected services: " +
                    String.join(", ", expectedServiceNames) +
                    ". Candidates: " + joinCandidatePaths(candidates));
        }

        throw new IOException(
            "No protobuf descriptor file found. Configure the compiler with " +
                "-A" + DESCRIPTOR_FILE_OPTION + "=<path-to-descriptor-set> " +
                "or -A" + DESCRIPTOR_PATH_OPTION + "=<descriptor-directory>. " +
                "Defaults searched: " + DEFAULT_DESCRIPTOR_RELATIVE_PATH + " in module, " +
                "module '" + DEFAULT_DESCRIPTOR_MODULE + "', and sibling modules.");
    }

    private void logSelectedDescriptor(Messager messager, Path descriptorPath) {
        if (messager == null || descriptorPath == null) {
            return;
        }
        messager.printMessage(
            Diagnostic.Kind.NOTE,
            "Using protobuf descriptor set file: " + descriptorPath.toAbsolutePath());
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
    private java.util.List<Path> collectDescriptorCandidates(Path baseDirectory) throws IOException {
        java.util.List<Path> candidates = new java.util.ArrayList<>();
        if (baseDirectory == null) {
            return candidates;
        }

        Path baseDir = baseDirectory.toAbsolutePath();
        addCandidatesFromModulePath(baseDir, candidates);
        addCandidatesFromModulePath(baseDir.resolve(DEFAULT_DESCRIPTOR_MODULE), candidates);

        Path parentDir = baseDir.getParent();
        if (parentDir != null) {
            addCandidatesFromModulePath(parentDir.resolve(DEFAULT_DESCRIPTOR_MODULE), candidates);

            try (var siblings = Files.list(parentDir)) {
                for (Path sibling : (Iterable<Path>) siblings.filter(Files::isDirectory)::iterator) {
                    addCandidatesFromModulePath(sibling, candidates);
                    addCandidatesFromModulePath(sibling.resolve(DEFAULT_DESCRIPTOR_MODULE), candidates);
                }
            }
        }

        collectDescriptorByWalk(baseDir, DEFAULT_SEARCH_DEPTH, candidates);
        if (parentDir != null) {
            collectDescriptorByWalk(parentDir, DEFAULT_SEARCH_DEPTH, candidates);
        }

        return candidates;
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

    private void addCandidatesFromModulePath(Path moduleDirectory, java.util.List<Path> candidates) throws IOException {
        if (moduleDirectory == null || !Files.exists(moduleDirectory) || !Files.isDirectory(moduleDirectory)) {
            return;
        }

        Path defaultDescriptorPath = moduleDirectory.resolve(DEFAULT_DESCRIPTOR_RELATIVE_PATH);
        if (Files.exists(defaultDescriptorPath) && Files.isReadable(defaultDescriptorPath)) {
            candidates.add(defaultDescriptorPath);
        }

        for (String defaultLocation : DEFAULT_LOCATIONS) {
            Path locationPath = moduleDirectory.resolve(defaultLocation);
            if (Files.exists(locationPath) && Files.isDirectory(locationPath)) {
                Optional<Path> descriptorFile = findDescriptorFile(locationPath);
                descriptorFile.ifPresent(candidates::add);
            }
        }
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

    private void collectDescriptorByWalk(Path baseDir, int maxDepth, java.util.List<Path> candidates) throws IOException {
        if (baseDir == null || !Files.exists(baseDir) || !Files.isDirectory(baseDir)) {
            return;
        }

        try (var paths = Files.walk(baseDir, maxDepth)) {
            for (Path dir : (Iterable<Path>) paths.filter(Files::isDirectory)::iterator) {
                Path candidate = dir.resolve(DEFAULT_DESCRIPTOR_RELATIVE_PATH);
                if (Files.exists(candidate) && Files.isReadable(candidate)) {
                    candidates.add(candidate);
                }
            }
        } catch (UncheckedIOException e) {
            // Ignore walk failures for heuristic search
        }
    }

    private java.util.List<Path> dedupeCandidates(java.util.List<Path> candidates) {
        java.util.LinkedHashSet<Path> unique = new java.util.LinkedHashSet<>(candidates);
        return new java.util.ArrayList<>(unique);
    }

    private String joinCandidatePaths(java.util.List<Path> candidates) {
        java.util.List<String> paths = new java.util.ArrayList<>();
        for (Path candidate : candidates) {
            paths.add(candidate.toString());
        }
        return String.join(", ", paths);
    }

    private boolean containsAnyService(
            DescriptorProtos.FileDescriptorSet descriptorSet,
            java.util.Set<String> expectedServiceNames) {
        if (descriptorSet == null || expectedServiceNames == null || expectedServiceNames.isEmpty()) {
            return false;
        }
        for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
            for (DescriptorProtos.ServiceDescriptorProto serviceProto : fileProto.getServiceList()) {
                if (expectedServiceNames.contains(serviceProto.getName())) {
                    return true;
                }
            }
        }
        return false;
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
