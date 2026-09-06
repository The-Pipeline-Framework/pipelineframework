package org.pipelineframework.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.pipelineframework.command.CommandReexecutionBoundary;
import org.pipelineframework.command.CommandReexecutionScope;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.execution.PipelineExecutionContext;

class CommandRetryAuthorityBoundaryTest {

    @Test
    void applicationFacingExecutionTypesDoNotExposeRetryAuthority() {
        assertNoRetryAuthorityComponents(PipelineExecutionContext.class.getRecordComponents());
        assertNoRetryAuthorityComponents(CommandRequest.class.getRecordComponents());

        boolean commandSupportExposesAuthority = Arrays.stream(CommandStepSupport.class.getMethods())
            .filter(method -> method.getDeclaringClass() != Object.class)
            .anyMatch(CommandRetryAuthorityBoundaryTest::mentionsRetryAuthority);
        assertFalse(commandSupportExposesAuthority);
    }

    @Test
    void retryScopeExposesOnlyOpaqueSnapshotPropagation() {
        Set<String> publicMethods = Arrays.stream(CommandReexecutionScope.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::getName)
            .collect(Collectors.toSet());

        assertEquals(Set.of("capture", "restore"), publicMethods);
        assertFalse(Arrays.stream(CommandReexecutionScope.Snapshot.class.getDeclaredConstructors())
            .map(Constructor::getModifiers)
            .anyMatch(Modifier::isPublic));
        assertFalse(Arrays.stream(CommandReexecutionScope.Snapshot.class.getDeclaredMethods())
            .anyMatch(method -> Modifier.isPublic(method.getModifiers())));
    }

    @Test
    void workerBoundaryPublishesNoAdmissionTypes() {
        Set<String> publicMethods = Arrays.stream(CommandReexecutionBoundary.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::getName)
            .collect(Collectors.toSet());
        assertEquals(Set.of("invokeTransitionWorker"), publicMethods);
        assertFalse(Arrays.stream(CommandReexecutionBoundary.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .anyMatch(CommandRetryAuthorityBoundaryTest::mentionsRetryAuthority));
        assertFalse(Arrays.stream(CommandReexecutionBoundary.class.getDeclaredConstructors())
            .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
    }

    private static void assertNoRetryAuthorityComponents(RecordComponent[] components) {
        assertFalse(Arrays.stream(components).anyMatch(component -> {
            String identity = component.getName() + " " + component.getType().getName();
            return identity.toLowerCase().contains("retry") || identity.toLowerCase().contains("admission");
        }));
    }

    private static boolean mentionsRetryAuthority(Method method) {
        return mentionsRetryAuthority(method.getReturnType())
            || Arrays.stream(method.getParameterTypes()).anyMatch(CommandRetryAuthorityBoundaryTest::mentionsRetryAuthority);
    }

    private static boolean mentionsRetryAuthority(Class<?> type) {
        return type.getName().contains("CommandReexecutionScope")
            || type.getName().contains("CommandReexecutionBoundary")
            || type.getName().contains("CommandRetryAdmission");
    }
}
