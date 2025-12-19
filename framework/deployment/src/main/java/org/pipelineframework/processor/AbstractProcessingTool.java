package org.pipelineframework.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Abstract base class providing common functionality for annotation processing tools.
 * Contains utility methods for annotation processing operations.
 *
 * Default constructor is provided by the abstract processor.
 */
public abstract class AbstractProcessingTool extends AbstractProcessor {
    /** Processing environment for this processor. */
    protected ProcessingEnvironment processingEnv;

    /**
     * Creates a new AbstractProcessingTool instance.
     * Default constructor provided by AbstractProcessor.
     */
    public AbstractProcessingTool() {
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
    }
    
    /**
     * Finds the AnnotationMirror instance for a specific annotation present on an element.
     *
     * @param element the element to inspect for the annotation
     * @param annotationClass the annotation class to look for
     * @return the matching {@link AnnotationMirror} if the annotation is present on the element, or {@code null} if not found
     */
    protected AnnotationMirror getAnnotationMirror(javax.lang.model.element.Element element, Class<?> annotationClass) {
        String annotationClassName = annotationClass.getCanonicalName();
        for (AnnotationMirror annotationMirror : element.getAnnotationMirrors()) {
            if (annotationMirror.getAnnotationType().toString().equals(annotationClassName)) {
                return annotationMirror;
            }
        }
        return null;
    }

    /**
     * Extracts a TypeMirror value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @return The TypeMirror value of the annotation member, or null if not found or if it's a void type
     */
    protected TypeMirror getAnnotationValue(AnnotationMirror annotation, String memberName) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof TypeMirror) {
                    return (TypeMirror) value;
                } else if (value instanceof String className) {
                    // Handle string values that should be class names
                    if ("void".equals(className) || className.isEmpty() || "java.lang.Void".equals(className)) {
                        // Return null for void types
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts a String value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @return The String value of the annotation member, or null if not found
     */
    protected String getAnnotationValueAsString(AnnotationMirror annotation, String memberName) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof String) {
                    return (String) value;
                } else if (value instanceof TypeMirror) {
                    // For class types, get the qualified name
                    return value.toString();
                }
            }
        }
        return null;
    }

    /**
     * Extracts a boolean value from an annotation by member name.
     *
     * @param annotation The annotation mirror to extract the value from
     * @param memberName The name of the annotation member to extract
     * @param defaultValue The default value to return if the annotation value is not found
     * @return The boolean value of the annotation member, or the default value if not found
     */
    protected boolean getAnnotationValueAsBoolean(AnnotationMirror annotation, String memberName, boolean defaultValue) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof Boolean) {
                    return (Boolean) value;
                }
                break; // Exit after finding the element even if it's not the correct type
            }
        }
        return defaultValue;
    }
}