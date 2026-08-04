package org.pipelineframework.connector.objectingest;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Injectable standard-stream boundary used by the stdio object providers.
 */
public record StandardStreams(InputStream stdin, OutputStream stdout) {

    public StandardStreams {
        stdin = Objects.requireNonNull(stdin, "stdin");
        stdout = Objects.requireNonNull(stdout, "stdout");
    }

    public static StandardStreams jvm() {
        return new StandardStreams(System.in, System.out);
    }
}
