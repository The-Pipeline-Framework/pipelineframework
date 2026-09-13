package org.pipelineframework.processor.util;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/**
 * Utility methods for deriving DTO type names from domain/service type names.
 */
public final class DtoTypeUtils {

    private DtoTypeUtils() {
    }

    /**
     * Converts a domain/service type into a DTO type by replacing common package segments and appending {@code Dto}.
     *
     * @param domainType input domain/service type
     * @return DTO type, or {@link ClassName#OBJECT} when input is null
     */
    public static TypeName toDtoType(TypeName domainType) {
        if (domainType == null) {
            return ClassName.OBJECT;
        }
        if (!(domainType instanceof ClassName domainClass)) {
            return ClassName.OBJECT;
        }

        String dtoPackagePath = domainClass.canonicalName()
            .replace(".domain.", ".dto.")
            .replace(".service.", ".dto.");

        int lastDot = dtoPackagePath.lastIndexOf('.');
        String packageName = lastDot > 0 ? dtoPackagePath.substring(0, lastDot) : "";
        String simpleName = lastDot > 0 ? dtoPackagePath.substring(lastDot + 1) : dtoPackagePath;
        return ClassName.get(packageName, simpleName + "Dto");
    }
}
