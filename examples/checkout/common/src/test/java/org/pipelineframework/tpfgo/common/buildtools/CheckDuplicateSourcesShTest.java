package org.pipelineframework.tpfgo.common.buildtools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for examples/checkout/build-tools/check-duplicate-sources.sh
 *
 * Locates the script relative to the working directory and invokes it via
 * a subprocess. Each test constructs temporary source trees and asserts exit codes.
 *
 * Tests that depend on bash process substitution are skipped automatically when
 * the feature is unavailable in the current execution environment (e.g. restricted sandboxes).
 */
class CheckDuplicateSourcesShTest {

    @TempDir
    Path tempDir;

    private Path scriptPath;
    private boolean processSubstitutionAvailable;

    @BeforeEach
    void setup() throws IOException, InterruptedException {
        scriptPath = locateScript();
        processSubstitutionAvailable = probeProcessSubstitution();
    }

    @Test
    void exitsTwoWhenCalledWithNoArguments() throws Exception {
        assumeScriptPresent();
        int exitCode = runScript(List.of());
        assertEquals(2, exitCode, "Script should exit 2 when no arguments are provided");
    }

    @Test
    void exitsTwoWhenCalledWithOnlyTargetDir() throws Exception {
        assumeScriptPresent();
        int exitCode = runScript(List.of(tempDir.toString()));
        assertEquals(2, exitCode, "Script should exit 2 when only one argument (target-dir) is provided");
    }

    @Test
    void exitsZeroWhenSourceDirIsEmpty() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcDir = tempDir.resolve("src-empty");
        Files.createDirectories(srcDir);
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcDir.toString()));
        assertEquals(0, exitCode, "Script should exit 0 when no Java files exist");
    }

    @Test
    void exitsZeroWhenSourceDirDoesNotExist() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path nonExistent = tempDir.resolve("no-such-dir");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), nonExistent.toString()));
        assertEquals(0, exitCode, "Script should exit 0 when source dir does not exist (skips it silently)");
    }

    @Test
    void exitsZeroWhenNoJavaFilesAreDuplicated() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcA = tempDir.resolve("src-a");
        Path srcB = tempDir.resolve("src-b");
        Files.createDirectories(srcA.resolve("com/example"));
        Files.createDirectories(srcB.resolve("com/example"));
        Files.writeString(srcA.resolve("com/example/Foo.java"), "class Foo {}");
        Files.writeString(srcB.resolve("com/example/Bar.java"), "class Bar {}");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcA.toString(), srcB.toString()));
        assertEquals(0, exitCode, "Script should exit 0 when all Java file paths are unique");
    }

    @Test
    void exitsOneWhenDuplicateJavaPathsDetected() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcA = tempDir.resolve("src-a");
        Path srcB = tempDir.resolve("src-b");
        Files.createDirectories(srcA.resolve("com/example"));
        Files.createDirectories(srcB.resolve("com/example"));
        // Same relative path in both source dirs
        Files.writeString(srcA.resolve("com/example/Duplicate.java"), "class Duplicate {}");
        Files.writeString(srcB.resolve("com/example/Duplicate.java"), "class Duplicate {}");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcA.toString(), srcB.toString()));
        assertEquals(1, exitCode, "Script should exit 1 when duplicate Java source paths are found");
    }

    @Test
    void exitsZeroWithSingleSourceDirContainingMultipleFiles() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcDir = tempDir.resolve("src");
        Files.createDirectories(srcDir.resolve("a/b"));
        Files.writeString(srcDir.resolve("a/b/Alpha.java"), "class Alpha {}");
        Files.writeString(srcDir.resolve("a/b/Beta.java"), "class Beta {}");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcDir.toString()));
        assertEquals(0, exitCode, "Script should exit 0 with unique files in a single source dir");
    }

    @Test
    void createsTargetDirIfMissing() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path targetDir = tempDir.resolve("new-target");
        Path srcDir = tempDir.resolve("src");
        Files.createDirectories(srcDir);

        runScript(List.of(targetDir.toString(), srcDir.toString()));
        assertTrue(Files.isDirectory(targetDir), "Script should create target directory if it does not exist");
    }

    // Regression: non-Java files in the same directory should not cause false duplicates
    @Test
    void ignoresNonJavaFiles() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcA = tempDir.resolve("src-a");
        Path srcB = tempDir.resolve("src-b");
        Files.createDirectories(srcA);
        Files.createDirectories(srcB);
        // Same relative name but not .java
        Files.writeString(srcA.resolve("Readme.md"), "# readme");
        Files.writeString(srcB.resolve("Readme.md"), "# readme");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcA.toString(), srcB.toString()));
        assertEquals(0, exitCode, "Script should ignore non-Java files when checking for duplicates");
    }

    // Regression: a .java file that is unique in each dir (different package paths) is not a duplicate
    @Test
    void sameClassNameInDifferentPackagesIsNotDuplicate() throws Exception {
        assumeScriptPresent();
        assumeProcessSubstitutionAvailable();
        Path srcA = tempDir.resolve("src-a");
        Path srcB = tempDir.resolve("src-b");
        Files.createDirectories(srcA.resolve("com/foo"));
        Files.createDirectories(srcB.resolve("com/bar"));
        Files.writeString(srcA.resolve("com/foo/Util.java"), "class Util {}");
        Files.writeString(srcB.resolve("com/bar/Util.java"), "class Util {}");
        Path targetDir = tempDir.resolve("target");

        int exitCode = runScript(List.of(targetDir.toString(), srcA.toString(), srcB.toString()));
        assertEquals(0, exitCode, "Same class name in different packages is not a duplicate path");
    }

    private int runScript(List<String> extraArgs) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command().add("bash");
        pb.command().add(scriptPath.toString());
        pb.command().addAll(extraArgs);
        pb.redirectErrorStream(false);
        Process proc = pb.start();
        // Drain output streams to prevent deadlock when OS pipe buffer fills
        proc.getInputStream().readAllBytes();
        proc.getErrorStream().readAllBytes();
        return proc.waitFor();
    }

    private void assumeScriptPresent() {
        assumeTrue(Files.exists(scriptPath),
            "check-duplicate-sources.sh not found at: " + scriptPath);
    }

    private void assumeProcessSubstitutionAvailable() {
        assumeTrue(processSubstitutionAvailable,
            "Bash process substitution (<(cmd)) not available in this environment; skipping");
    }

    private static boolean probeProcessSubstitution() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", "cat < <(echo test)");
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        proc.getInputStream().readAllBytes();
        return proc.waitFor() == 0;
    }

    private static Path locateScript() {
        Path userDir = Path.of(System.getProperty("user.dir", ".")).normalize();
        // common module working dir is examples/checkout/common
        Path[] candidates = {
            userDir.resolve("../build-tools/check-duplicate-sources.sh").normalize(),
            userDir.resolve("build-tools/check-duplicate-sources.sh").normalize(),
            userDir.resolve("../../build-tools/check-duplicate-sources.sh").normalize(),
            userDir.resolve("examples/checkout/build-tools/check-duplicate-sources.sh").normalize(),
        };
        for (Path c : candidates) {
            if (Files.exists(c)) {
                return c;
            }
        }
        return candidates[0];
    }
}