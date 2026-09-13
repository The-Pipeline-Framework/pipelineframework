package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Element;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

/**
 * Simple Filer implementation that writes generated files to a temp directory.
 */
final class TestFiler implements Filer {

    private final Path outputDir;

    TestFiler(Path outputDir) {
        this.outputDir = outputDir;
    }

    @Override
    public JavaFileObject createSourceFile(CharSequence name, Element... originatingElements) throws IOException {
        return new PathJavaFileObject(outputDir, name.toString(), JavaFileObject.Kind.SOURCE);
    }

    @Override
    public JavaFileObject createClassFile(CharSequence name, Element... originatingElements) {
        throw new UnsupportedOperationException("Class file generation is not supported in tests.");
    }

    @Override
    public FileObject createResource(JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName,
                                     Element... originatingElements) {
        throw new UnsupportedOperationException("Resource generation is not supported in tests.");
    }

    @Override
    public FileObject getResource(JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName) {
        throw new UnsupportedOperationException("Resource access is not supported in tests.");
    }

    private static final class PathJavaFileObject extends SimpleJavaFileObject {
        private final Path path;

        private PathJavaFileObject(Path outputDir, String name, JavaFileObject.Kind kind) {
            super(outputDir.resolve(name.replace('.', '/') + kind.extension).toUri(), kind);
            this.path = outputDir.resolve(name.replace('.', '/') + kind.extension);
        }

        @Override
        public Writer openWriter() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newBufferedWriter(path);
        }

        @Override
        public OutputStream openOutputStream() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newOutputStream(path);
        }
    }
}
