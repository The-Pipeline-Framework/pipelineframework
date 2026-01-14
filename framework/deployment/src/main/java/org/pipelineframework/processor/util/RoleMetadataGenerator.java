package org.pipelineframework.processor.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.javapoet.ClassName;

/**
 * Utility class to generate role metadata for pipeline artifacts.
 * This generates a META-INF/pipeline/roles.json file for documentation
 * and optional validation; classifier packaging should rely on role-based
 * source/output directories instead of this metadata.
 */
public class RoleMetadataGenerator {

    private final ProcessingEnvironment processingEnv;
    private final Map<String, String> classToRoleMap = new HashMap<>();
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Creates a new RoleMetadataGenerator.
     *
     * @param processingEnv the processing environment for compiler utilities and messaging
     */
    public RoleMetadataGenerator(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }

    /**
     * Records a class with its role.
     *
     * @param className the fully qualified class name
     * @param role the role of the class
     */
    public void recordClassWithRole(ClassName className, String role) {
        classToRoleMap.put(className.reflectionName(), role);
    }

    /**
     * Records a class with its role.
     *
     * @param className the fully qualified class name as a string
     * @param role the role of the class
     */
    public void recordClassWithRole(String className, String role) {
        classToRoleMap.put(className, role);
    }

    /**
     * Writes the role metadata to META-INF/pipeline/roles.json.
     *
     * @throws IOException if writing the file fails
     */
    public void writeRoleMetadata() throws IOException {
        // Group classes by role
        Map<String, java.util.List<String>> rolesToClasses = new HashMap<>();
        for (Map.Entry<String, String> entry : classToRoleMap.entrySet()) {
            String className = entry.getKey();
            String role = entry.getValue();
            
            rolesToClasses.computeIfAbsent(role, k -> new java.util.ArrayList<>()).add(className);
        }

        // Create the metadata structure
        RoleMetadata metadata = new RoleMetadata(rolesToClasses);

        // Write to file
        javax.tools.FileObject resourceFile = processingEnv.getFiler()
            .createResource(StandardLocation.CLASS_OUTPUT, "", "META-INF/pipeline/roles.json", (javax.lang.model.element.Element[]) null);

        try (var writer = resourceFile.openWriter()) {
            writer.write(gson.toJson(metadata));
        }
    }

    /**
     * Inner class to represent the role metadata structure.
     */
    private static class RoleMetadata {
        Map<String, java.util.List<String>> roles;

        RoleMetadata(Map<String, java.util.List<String>> roles) {
            this.roles = roles;
        }
    }
}
