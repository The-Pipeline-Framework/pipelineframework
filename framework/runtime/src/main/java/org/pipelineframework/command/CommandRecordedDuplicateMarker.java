package org.pipelineframework.command;

import java.lang.reflect.Method;

/**
 * Best-effort marker for command outputs that expose a recordedDuplicate flag.
 */
final class CommandRecordedDuplicateMarker {
    private CommandRecordedDuplicateMarker() {
    }

    static void mark(Object output) {
        if (output == null) {
            return;
        }
        if (invoke(output, Boolean.class, Boolean.TRUE)) {
            return;
        }
        invoke(output, boolean.class, true);
    }

    private static boolean invoke(Object output, Class<?> parameterType, Object value) {
        try {
            Method method = output.getClass().getMethod("setRecordedDuplicate", parameterType);
            method.invoke(output, value);
            return true;
        } catch (ReflectiveOperationException ignored) {
            // Output types do not have to expose a duplicate marker.
            return false;
        }
    }
}
