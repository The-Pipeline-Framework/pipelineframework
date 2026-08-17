package org.pipelineframework.connector.maven;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderArtifacts;

/** Generates connector artifact metadata from side-effect-free provider construction. */
@Mojo(
    name = "generate-provider-artifacts",
    defaultPhase = LifecyclePhase.PROCESS_CLASSES,
    requiresDependencyResolution = ResolutionScope.RUNTIME,
    threadSafe = true
)
public final class GenerateConnectorProviderArtifactsMojo extends AbstractMojo {
    @Parameter(defaultValue = "${project.build.outputDirectory}", readonly = true, required = true)
    private File outputDirectory;

    @Parameter(defaultValue = "${project.runtimeClasspathElements}", readonly = true, required = true)
    private List<String> runtimeClasspathElements;

    @Override
    public void execute() throws MojoExecutionException {
        Path classes = outputDirectory.toPath();
        if (!Files.isDirectory(classes)) {
            getLog().debug("No compiled connector classes found at " + classes);
            return;
        }
        try (URLClassLoader loader = classLoader()) {
            List<ConnectorProvider<?>> providers = providers(classes, loader);
            if (providers.isEmpty()) {
                deleteGeneratedArtifacts(classes);
                getLog().debug("No ConnectorProvider implementations found in " + classes);
                return;
            }
            ConnectorProviderArtifacts.write(classes, providers);
            getLog().info("Generated connector provider artifacts for " + providers.size() + " provider(s)");
        } catch (IOException | ReflectiveOperationException exception) {
            throw new MojoExecutionException("Unable to generate connector provider artifacts", exception);
        }
    }

    static void deleteGeneratedArtifacts(Path classes) throws IOException {
        Files.deleteIfExists(classes.resolve(ConnectorProviderArtifacts.SERVICE_PATH));
        Files.deleteIfExists(classes.resolve(
            org.pipelineframework.connector.ConnectorProviderManifestLoader.RESOURCE_PATH));
    }

    private URLClassLoader classLoader() throws IOException {
        List<URL> urls = new ArrayList<>();
        for (String element : runtimeClasspathElements) {
            urls.add(Path.of(element).toUri().toURL());
        }
        return new URLClassLoader(urls.toArray(URL[]::new), getClass().getClassLoader());
    }

    private static List<ConnectorProvider<?>> providers(Path classes, ClassLoader loader)
        throws IOException, ReflectiveOperationException {
        List<ConnectorProvider<?>> providers = new ArrayList<>();
        try (var paths = Files.walk(classes)) {
            for (Path path : paths.filter(value -> value.toString().endsWith(".class")).sorted().toList()) {
                String className = classes.relativize(path).toString()
                    .replace(File.separatorChar, '.')
                    .replaceAll("\\.class$", "");
                if (className.contains("$") || className.equals("module-info") || className.equals("package-info")) {
                    continue;
                }
                Class<?> type = Class.forName(className, false, loader);
                if (ConnectorProvider.class.isAssignableFrom(type)
                    && Modifier.isPublic(type.getModifiers())
                    && !Modifier.isAbstract(type.getModifiers())) {
                    providers.add((ConnectorProvider<?>) type.getConstructor().newInstance());
                }
            }
        }
        providers.sort(Comparator.comparing(provider -> provider.getClass().getName()));
        return List.copyOf(providers);
    }
}
