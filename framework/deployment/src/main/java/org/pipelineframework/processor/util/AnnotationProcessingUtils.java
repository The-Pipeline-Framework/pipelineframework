package org.pipelineframework.processor.util;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Utility class containing common methods for annotation processing operations.
 */
public final class AnnotationProcessingUtils {

    private AnnotationProcessingUtils() {
        // Prevent instantiation
    }

    /**
     * Finds the AnnotationMirror instance for a specific annotation present on an element.
     *
     * @param element the element to inspect for the annotation
     * @param annotationClass the annotation class to look for
     * @return the matching {@link AnnotationMirror} if the annotation is present on the element, or {@code null} if not found
     */
    public static AnnotationMirror getAnnotationMirror(Element element, Class<?> annotationClass) {
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
    public static TypeMirror getAnnotationValue(AnnotationMirror annotation, String memberName) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                javax.lang.model.element.AnnotationValue annotationValue = annotation.getElementValues().get(executableElement);

                // Properly extract the value from AnnotationValue
                Object value = annotationValue.getValue();

                // For Class<?> values in annotations, the value is typically a TypeMirror wrapped in AnnotationValue
                if (value instanceof TypeMirror) {
                    return (TypeMirror) value;
                } else if (value instanceof javax.lang.model.element.AnnotationValue) {
                    // Sometimes the value is doubly wrapped
                    Object unwrappedValue = ((javax.lang.model.element.AnnotationValue) value).getValue();
                    if (unwrappedValue instanceof TypeMirror) {
                        return (TypeMirror) unwrappedValue;
                    }
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
    public static boolean getAnnotationValueAsBoolean(AnnotationMirror annotation, String memberName, boolean defaultValue) {
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