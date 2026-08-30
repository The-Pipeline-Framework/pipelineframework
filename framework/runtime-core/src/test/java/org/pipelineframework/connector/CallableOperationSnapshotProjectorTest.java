package org.pipelineframework.connector;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CallableOperationSnapshotProjectorTest {
    private static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("snapshot.provider");
    private static final ConnectorProviderDescriptor PROVIDER = new ConnectorProviderDescriptor(
        PROVIDER_ID, new ConnectorProviderVersion(1, 0));
    private static final ConnectorOperationTypeContract TYPES = new ConnectorOperationTypeContract(
        "snapshot.LookupInput", Optional.of("snapshot.LookupOutput"));

    @Test
    void projectsOnlyAuthorizedStaticMetadataInStableIdentityOrder() {
        CommandCapabilities commandCapabilities = new CommandCapabilities(
            true, true, true, CommandExecutionPosture.ATTENDED,
            CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, true, Set.of("provider.receipt"));
        QueryCapabilities queryCapabilities = new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.of(Duration.ofMinutes(5)), Optional.of(Duration.ofSeconds(10)));
        ConnectorOperationDescriptor command = descriptor(
            "change", ConnectorOperationKind.COMMAND, Optional.of(commandCapabilities), Optional.empty(), Optional.of(TYPES));
        ConnectorOperationDescriptor query = descriptor(
            "lookup", ConnectorOperationKind.QUERY, Optional.empty(), Optional.of(queryCapabilities), Optional.of(TYPES));
        ConnectorOperationDescriptor denied = descriptor(
            "delete", ConnectorOperationKind.COMMAND, Optional.empty(), Optional.empty(), Optional.of(TYPES));

        CallableOperationSnapshot snapshot = new CallableOperationSnapshotProjector().project(
            catalog(command, query, denied),
            new CallableOperationAuthorization(Set.of(identity(query), identity(command))));

        assertEquals(List.of("change", "lookup"), snapshot.operations().stream()
            .map(operation -> operation.identity().operationId()).toList());
        assertFalse(snapshot.operations().stream().anyMatch(operation -> "delete".equals(operation.identity().operationId())));
        CallableCommandCapabilities projectedCommand = snapshot.operations().getFirst().commandCapabilities().orElseThrow();
        assertEquals(CommandExecutionPosture.ATTENDED, projectedCommand.executionPosture());
        assertEquals(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, projectedCommand.maximumMachineConfirmation());
        assertTrue(projectedCommand.userConfirmationSupported());
        CallableQueryCapabilities projectedQuery = snapshot.operations().get(1).queryCapabilities().orElseThrow();
        assertEquals(QueryCacheability.CACHEABLE, projectedQuery.cacheability());
        assertEquals(Optional.of(Duration.ofMinutes(5)), projectedQuery.maximumCacheAge());
        assertEquals(Set.of("executionPosture", "maximumMachineConfirmation", "userConfirmationSupported"),
            java.util.Arrays.stream(CallableCommandCapabilities.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName).collect(java.util.stream.Collectors.toSet()));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.operations().add(snapshot.operations().getFirst()));
    }

    @Test
    void derivesNormalizedTypesFromFrameworkGeneratedOperationMetadata() {
        ConnectorOperationDescriptor descriptor = ConnectorDescriptors.operation(new LookupQuery());

        assertEquals(new ConnectorOperationTypeContract("string", Optional.of("list<integer>")),
            descriptor.typeContract().orElseThrow());
        String json = ConnectorProviderArtifacts.json(new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
            List.of(new ConnectorProviderArtifactDescriptor(PROVIDER, List.of(descriptor)))));
        assertTrue(json.contains("\"typeContract\":{\"input\":\"string\",\"output\":\"list<integer>\"}"));
    }

    @Test
    void failsClosedForUnknownUnsupportedAndIncompleteAuthorizedMetadata() {
        ConnectorOperationDescriptor missingTypes = descriptor(
            "legacy", ConnectorOperationKind.QUERY, Optional.empty(), Optional.empty(), Optional.empty());
        ConnectorOperationDescriptor unsupported = descriptor(
            "source", ConnectorOperationKind.OBJECT_SOURCE, Optional.empty(), Optional.empty(), Optional.of(TYPES));
        CallableOperationSnapshotProjector projector = new CallableOperationSnapshotProjector();

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> projector.project(
            catalog(missingTypes), new CallableOperationAuthorization(Set.of(identity(missingTypes)))));
        assertTrue(missing.getMessage().contains("no normalized type contract"));

        IllegalArgumentException wrongKind = assertThrows(IllegalArgumentException.class, () -> projector.project(
            catalog(unsupported), new CallableOperationAuthorization(Set.of(identity(unsupported)))));
        assertTrue(wrongKind.getMessage().contains("not callable"));

        ConnectorOperationIdentity unknown = new ConnectorOperationIdentity(
            PROVIDER_ID, "unknown", ConnectorOperationKind.QUERY, 1);
        IllegalArgumentException absent = assertThrows(IllegalArgumentException.class, () -> projector.project(
            catalog(missingTypes), new CallableOperationAuthorization(Set.of(unknown))));
        assertTrue(absent.getMessage().contains("no static metadata"));
    }

    @Test
    void rejectsStreamingQueryFromTheUnaryCallableSnapshot() {
        ConnectorOperationDescriptor streaming = new ConnectorOperationDescriptor(
            "find.many",
            ConnectorOperationKind.QUERY,
            1,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(QueryOperationCardinality.ONE_TO_MANY),
            Optional.of(TYPES));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () ->
            new CallableOperationSnapshotProjector().project(
                catalog(streaming),
                new CallableOperationAuthorization(Set.of(identity(streaming)))));

        assertTrue(failure.getMessage().contains("not callable in unary snapshot"));
    }

    @Test
    void digestIsOrderIndependentAndChangesWithModelVisibleMetadata() {
        CallableOperationDefinition baseline = queryDefinition(
            new ConnectorOperationIdentity(PROVIDER_ID, "lookup", ConnectorOperationKind.QUERY, 1), TYPES,
            QueryCapabilities.cacheable());
        CallableOperationDefinition second = queryDefinition(
            new ConnectorOperationIdentity(PROVIDER_ID, "other", ConnectorOperationKind.QUERY, 1), TYPES,
            QueryCapabilities.cacheable());

        assertEquals(
            CallableOperationSnapshot.of(List.of(baseline, second)).identity(),
            CallableOperationSnapshot.of(List.of(second, baseline)).identity());
        assertDigestChanges(baseline, queryDefinition(
            new ConnectorOperationIdentity(PROVIDER_ID, "lookup", ConnectorOperationKind.QUERY, 2), TYPES,
            QueryCapabilities.cacheable()));
        assertDigestChanges(baseline, queryDefinition(
            baseline.identity(), new ConnectorOperationTypeContract("snapshot.OtherInput", TYPES.outputType()),
            QueryCapabilities.cacheable()));
        assertDigestChanges(baseline, queryDefinition(
            baseline.identity(), TYPES,
            new QueryCapabilities(QueryCacheability.CACHEABLE, Optional.of(Duration.ofMinutes(1)), Optional.empty())));
    }

    @Test
    void directDefinitionsRejectUnsupportedKindsAndRuntimeMetadataTypes() {
        ConnectorOperationIdentity source = new ConnectorOperationIdentity(
            PROVIDER_ID, "source", ConnectorOperationKind.OBJECT_SOURCE, 1);
        assertThrows(IllegalArgumentException.class, () -> new CallableOperationDefinition(
            source, "source operation", TYPES, Optional.empty(), Optional.empty()));

        Set<Class<?>> componentTypes = java.util.Arrays.stream(CallableOperationDefinition.class.getRecordComponents())
            .map(java.lang.reflect.RecordComponent::getType).collect(java.util.stream.Collectors.toSet());
        assertFalse(componentTypes.contains(ConnectorOperationDescriptor.class));
        assertFalse(componentTypes.contains(ConnectorConfigSchemaDescriptor.class));
        assertFalse(componentTypes.contains(ConnectionRef.class));
        assertFalse(componentTypes.contains(SecretRef.class));
    }

    private static CallableOperationDefinition queryDefinition(
        ConnectorOperationIdentity identity,
        ConnectorOperationTypeContract types,
        QueryCapabilities capabilities
    ) {
        return new CallableOperationDefinition(
            identity, "query operation " + identity.operationId(), types, Optional.empty(),
            Optional.of(CallableQueryCapabilities.from(capabilities)));
    }

    private static void assertDigestChanges(CallableOperationDefinition first, CallableOperationDefinition second) {
        assertNotEquals(CallableOperationSnapshot.of(List.of(first)).identity(),
            CallableOperationSnapshot.of(List.of(second)).identity());
    }

    private static ConnectorProviderManifestCatalog catalog(ConnectorOperationDescriptor... operations) {
        return new ConnectorProviderManifestCatalog(List.of(new ConnectorProviderManifest(
            ConnectorProviderManifest.CURRENT_SCHEMA_VERSION,
            List.of(new ConnectorProviderArtifactDescriptor(PROVIDER, List.of(operations))))));
    }

    private static ConnectorOperationDescriptor descriptor(
        String id,
        ConnectorOperationKind kind,
        Optional<CommandCapabilities> commandCapabilities,
        Optional<QueryCapabilities> queryCapabilities,
        Optional<ConnectorOperationTypeContract> typeContract
    ) {
        return new ConnectorOperationDescriptor(
            id, kind, 1, Optional.empty(), commandCapabilities, queryCapabilities, typeContract);
    }

    private static ConnectorOperationIdentity identity(ConnectorOperationDescriptor descriptor) {
        return ConnectorOperationIdentity.of(PROVIDER, descriptor);
    }

    private static final class LookupQuery implements QueryOperation<String, Object, List<Integer>> {
        @Override
        public String id() {
            return "lookup";
        }

        @Override
        public CompletionStage<QueryOutcome<List<Integer>>> query(
            QueryInvocation<String, Object, List<Integer>> invocation
        ) {
            return CompletableFuture.completedFuture(new QueryOutcome.Found<>(List.of()));
        }
    }
}
