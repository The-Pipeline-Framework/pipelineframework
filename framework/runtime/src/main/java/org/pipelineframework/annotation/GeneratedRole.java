package org.pipelineframework.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark generated classes with their role in the pipeline framework.
 * This is used to generate role metadata that Maven can use for classifier-based packaging.
 */
@Retention(RetentionPolicy.SOURCE) // Only needed at compile time
@Target(ElementType.TYPE) // Applies to classes
public @interface GeneratedRole {
    /**
     * The role of the generated class in the pipeline framework.
     *
     * @return the generated role
     */
    Role value();

    /**
     * Enumeration of possible roles for generated classes.
     */
    enum Role {
        /**
         * gRPC client steps for orchestrator services
         */
        ORCHESTRATOR_CLIENT,
        
        /**
         * gRPC server adapters for application pipeline steps
         */
        PIPELINE_SERVER,
        
        /**
         * Client stubs for calling external plugin services
         */
        PLUGIN_CLIENT,
        
        /**
         * Server implementations for external plugin services
         */
        PLUGIN_SERVER,
        
        /**
         * REST resources for HTTP-based access to pipeline steps
         */
        REST_SERVER
    }
}
