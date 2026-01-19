package org.pipelineframework.processor.util;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Utility class containing common methods for annotation processing operations.
 */
public final class AnnotationProcessingUtils {

    /**
     * Prevents instantiation of this utility class.
     */
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
     * Retrieve the TypeMirror value of a named member from an AnnotationMirror.
     *
     * @param annotation the AnnotationMirror to inspect
     * @param memberName the simple name of the annotation member to read
     * @return the TypeMirror of the specified member, or {@code null} if the member is absent or its value is not a TypeMirror
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
                break; // Exit after finding the element even if it's not the correct type
            }
        }
        return null;
    }

    /**
     * Obtain a boolean member value from an annotation, returning a provided fallback when the member is absent or not a boolean.
     *
     * @param annotation   the annotation mirror to read the member from
     * @param memberName   the simple name of the annotation member to look up
     * @param defaultValue the value to return if the member is not present or is not a boolean
     * @return `true` if the specified annotation member has the boolean value `true`, `false` otherwise; returns the supplied `defaultValue` when the member is absent or not a boolean
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

    /**
     * Obtain a string member value from an annotation, returning a provided fallback when the member is absent or not a string.
     *
     * @param annotation   the annotation mirror to read the member from
     * @param memberName   the simple name of the annotation member to look up
     * @param defaultValue the value to return if the member is not present or is not a string
     * @return the string value of the specified annotation member, or {@code defaultValue} when absent or not a string
     */
    public static String getAnnotationValueAsString(AnnotationMirror annotation, String memberName, String defaultValue) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof String) {
                    return (String) value;
                }
                if (value instanceof javax.lang.model.element.AnnotationValue annotationValue) {
                    Object unwrapped = annotationValue.getValue();
                    if (unwrapped instanceof String) {
                        return (String) unwrapped;
                    }
                }
                break; // Exit after finding the element even if it's not the correct type
            }
        }
        return defaultValue;
    }

    /**
     * Obtain an enum member value from an annotation, returning a provided fallback when absent or not resolvable.
     *
     * @param annotation   the annotation mirror to read the member from
     * @param memberName   the simple name of the annotation member to look up
     * @param enumType     the enum class to resolve values for
     * @param defaultValue the value to return when absent or not resolvable
     * @return the enum value, or {@code defaultValue} when absent or unresolvable
     */
    public static <T extends Enum<T>> T getAnnotationValueAsEnum(
        AnnotationMirror annotation, String memberName, Class<T> enumType, T defaultValue) {
        for (ExecutableElement executableElement : annotation.getElementValues().keySet()) {
            if (executableElement.getSimpleName().toString().equals(memberName)) {
                Object value = annotation.getElementValues().get(executableElement).getValue();
                if (value instanceof javax.lang.model.element.VariableElement variableElement) {
                    String name = variableElement.getSimpleName().toString();
                    return Enum.valueOf(enumType, name);
                }
                if (value instanceof String stringValue) {
                    return Enum.valueOf(enumType, stringValue);
                }
                if (value instanceof javax.lang.model.element.AnnotationValue annotationValue) {
                    Object unwrapped = annotationValue.getValue();
                    if (unwrapped instanceof javax.lang.model.element.VariableElement variableElement) {
                        String name = variableElement.getSimpleName().toString();
                        return Enum.valueOf(enumType, name);
                    }
                    if (unwrapped instanceof String stringValue) {
                        return Enum.valueOf(enumType, stringValue);
                    }
                }
                break;
            }
        }
        return defaultValue;
    }
}
