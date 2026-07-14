package org.pipelineframework.stdio.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class StdioObjectDemoIT {

  private static final Duration TIMEOUT = Duration.ofMinutes(1);

  @Test
  void publishesOneJsonValueWithoutContaminatingStandardOutput() throws Exception {
    ProcessResult result = run("{\"name\":\"Mariano\"}");

    assertEquals(0, result.exitCode());
    assertEquals("{\"greetings\":[\"Hello, Mariano!\"]}", result.stdout());
    assertTrue(result.stderr().contains("Object Ingest submitted 1 execution(s)"));
  }

  @Test
  void mapsAJsonCollectionAsOneTypedPipelineAdmission() throws Exception {
    ProcessResult result = run("[{\"name\":\"Mariano\"},{\"name\":\"Ada\"}]");

    assertEquals(0, result.exitCode());
    assertEquals("{\"greetings\":[\"Hello, Mariano!\",\"Hello, Ada!\"]}", result.stdout());
  }

  @Test
  void reportsMalformedJsonWithoutPublishingBusinessOutput() throws Exception {
    ProcessResult result = run("{broken");

    assertEquals(1, result.exitCode());
    assertEquals("", result.stdout());
    assertTrue(result.stderr().contains("Object Ingest failed"));
  }

  private ProcessResult run(String input) throws Exception {
    Path basedir = Path.of(System.getProperty("stdio.demo.basedir"));
    Path application = basedir.resolve("target/quarkus-app/quarkus-run.jar");
    ProcessBuilder builder = new ProcessBuilder(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-jar", application.toString(), "--ingest-once", "--async-timeout-minutes", "1");
    builder.environment().put("PIPELINE_CONFIG", basedir.resolve("pipeline.yaml").toString());
    Process process = builder.start();
    process.getOutputStream().write(input.getBytes(StandardCharsets.UTF_8));
    process.getOutputStream().close();

    boolean completed = process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    if (!completed) {
      process.destroyForcibly();
      throw new IOException("stdio demo did not finish within " + TIMEOUT);
    }
    return new ProcessResult(
        process.exitValue(),
        new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8),
        new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
  }

  private record ProcessResult(int exitCode, String stdout, String stderr) {
  }
}
