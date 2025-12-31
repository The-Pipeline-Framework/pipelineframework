package org.pipelineframework.maven;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * Mojo to generate classifier JARs based on role metadata.
 */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.PACKAGE)
public class ClassifierGeneratorMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    private MavenSession session;

    @Parameter(property = "rolesMetadataPath", defaultValue = "META-INF/pipeline/roles.json")
    private String rolesMetadataPath;

    @Parameter
    private List<ClassifierArtifactConfig> classifierArtifacts = new ArrayList<>();

    public void execute() throws MojoExecutionException {
        getLog().info("Reading role metadata from: " + rolesMetadataPath);

        try {
            // Read the roles.json file
            File rolesFile = new File(project.getBuild().getOutputDirectory(), rolesMetadataPath);
            if (!rolesFile.exists()) {
                getLog().warn("Roles metadata file does not exist: " + rolesFile.getAbsolutePath());
                return;
            }

            // Parse the JSON content
            Gson gson = new Gson();
            Map<String, Map<String, List<String>>> metadata;
            try (FileInputStream fis = new FileInputStream(rolesFile)) {
                metadata = gson.fromJson(new java.io.InputStreamReader(fis),
                    new TypeToken<Map<String, Map<String, List<String>>>>(){}.getType());
            }

            // Generate classifier JARs based on role metadata
            for (ClassifierArtifactConfig config : classifierArtifacts) {
                generateClassifierJar(metadata, config);
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Failed to read roles metadata", e);
        }
    }

    private void generateClassifierJar(Map<String, Map<String, List<String>>> metadata,
                                     ClassifierArtifactConfig config) throws MojoExecutionException {
        getLog().info("Generating classifier JAR for role: " + config.getRole() +
                     " with classifier: " + config.getClassifier());

        // Get the classes for this role
        Map<String, List<String>> rolesMap = metadata.get("roles");
        if (rolesMap == null) {
            getLog().warn("No 'roles' key found in metadata");
            return;
        }

        List<String> classNames = rolesMap.get(config.getRole());
        if (classNames == null || classNames.isEmpty()) {
            getLog().warn("No classes found for role: " + config.getRole());
            return;
        }

        getLog().info("Found " + classNames.size() + " classes for role " + config.getRole());
        for (String className : classNames) {
            getLog().debug("  - " + className);
        }

        // Create a JAR with only the classes that match this role
        createRoleBasedJar(config.getClassifier(), classNames);
    }

    private void createRoleBasedJar(String classifier, List<String> classNames) throws MojoExecutionException {
        try {
            // Create the JAR file with the specified classifier
            String finalName = project.getBuild().getFinalName();
            File jarFile = new File(project.getBuild().getDirectory(),
                                  finalName + "-" + classifier + ".jar");

            // Use Maven Archiver to create the JAR
            org.apache.maven.archiver.MavenArchiver archiver = new org.apache.maven.archiver.MavenArchiver();
            archiver.setArchiver(new org.codehaus.plexus.archiver.jar.JarArchiver());
            archiver.setOutputFile(jarFile);

            // Add only the classes that match the role to the JAR
            File outputDirectory = new File(project.getBuild().getOutputDirectory());
            for (String className : classNames) {
                // Convert class name to file path
                String classFilePath = className.replace('.', '/') + ".class";
                File classFile = new File(outputDirectory, classFilePath);

                if (classFile.exists()) {
                    archiver.getArchiver().addFile(classFile, classFilePath);
                } else {
                    getLog().warn("Class file does not exist: " + classFile.getAbsolutePath());
                }
            }

            // Also include the role metadata and any other necessary resources
            File rolesFile = new File(outputDirectory, "META-INF/pipeline/roles.json");
            if (rolesFile.exists()) {
                archiver.getArchiver().addFile(rolesFile, "META-INF/pipeline/roles.json");
            }

            // Create the archive using the MavenSession and MavenArchiveConfiguration
            archiver.createArchive(session, project, null);

            getLog().info("Created classifier JAR: " + jarFile.getAbsolutePath());

        } catch (Exception e) {
            throw new MojoExecutionException("Failed to create classifier JAR", e);
        }
    }
}

/**
 * Configuration for a classifier artifact.
 */
@Setter
@Getter
class ClassifierArtifactConfig {
    private String classifier;
    private String role;

}