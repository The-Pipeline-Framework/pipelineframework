package org.pipelineframework.host.gmail;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStreamReader;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.spec.SecretKeySpec;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.gmail.Gmail;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.gmail.AuthenticatedGmailConnection;
import org.pipelineframework.connector.gmail.GmailQueryConnector;

class GmailConnectionsTest {
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path temporary;
    private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(8);
    private final MutableClock clock = new MutableClock();
    private final FakeAuthorization provider = new FakeAuthorization(clock);
    private final List<GmailConnections> managers = new ArrayList<>();
    private JdbcDataSource database;
    private ConnectionEncryption encryption;
    private JdbcGmailConnectionStore store;
    private GmailConnections connections;

    @BeforeEach
    void setup() throws Exception {
        database = new JdbcDataSource();
        database.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        try (var connection = database.getConnection(); var script = new InputStreamReader(
            getClass().getResourceAsStream("/META-INF/tpf-gmail-connections.sql"), java.nio.charset.StandardCharsets.UTF_8)) {
            RunScript.execute(connection, script);
        }
        encryption = new ConnectionEncryption("key-1", Map.of("key-1", new SecretKeySpec(new byte[32], "AES")));
        store = new JdbcGmailConnectionStore(database, encryption, "test-client-id");
        connections = manager(store, provider);
    }

    @AfterEach
    void shutdown() {
        provider.release.countDown();
        managers.forEach(GmailConnections::close);
        executor.shutdownNow();
    }

    @Test
    void tenantAliasesSelectDistinctAccountsAndReuseClientsAcrossLiveResolutions() {
        connect(connections, key("tenant-a"), "account-a");
        connect(connections, key("tenant-b"), "account-b");
        var a = join(connections.resolve(request("tenant-a")));
        assertSame(a, join(connections.resolve(request("tenant-a"))));
        var b = join(connections.resolve(request("tenant-b")));
        assertNotSame(a, b);
        assertEquals("account-a", store.read(key("tenant-a")).orElseThrow().grant().orElseThrow().subject());
        assertEquals(2, provider.clients.get());
        assertThrows(CompletionException.class, () -> join(connections.resolve(request("unknown"))));
    }

    @Test
    void oneGoogleGrantCannotAcquireTwoIndependentLogicalOwners() {
        connect(connections, key("tenant-a"), "same-account");
        assertThrows(CompletionException.class, () -> connect(connections, key("tenant-b"), "same-account"));
        assertEquals(GmailConnections.Phase.REQUIRES_REAUTHORIZATION, join(connections.status(key("tenant-b"))).phase());
        join(connections.disconnect(key("tenant-a")));
        assertThrows(CompletionException.class, () -> connect(connections, key("tenant-b"), "same-account"));
    }

    @Test
    void callbackIsBoundToActorBrowserTenantExpiryAndOneUse() {
        ConnectionKey key = key("tenant-a");
        var start = join(connections.begin(key, "actor"));
        String state = state(start);
        assertThrows(CompletionException.class, () -> join(connections.complete(key, "other", state, start.browserSecret(), "account")));
        assertThrows(CompletionException.class, () -> join(connections.complete(key, "actor", state, "wrong-browser", "account")));
        assertThrows(CompletionException.class, () -> join(connections.complete(key, "actor", "wrong-state", start.browserSecret(), "account")));
        assertThrows(CompletionException.class, () -> join(connections.complete(key("tenant-b"), "actor", state, start.browserSecret(), "account")));
        assertEquals(0, provider.exchanges.get());
        join(connections.complete(key, "actor", state, start.browserSecret(), "account"));
        assertThrows(CompletionException.class, () -> join(connections.complete(key, "actor", state, start.browserSecret(), "account")));
        assertEquals(1, provider.exchanges.get());
        var expired = join(connections.begin(key, "actor"));
        clock.advance(600_001);
        assertThrows(CompletionException.class, () -> join(connections.complete(key, "actor", state(expired), expired.browserSecret(), "account")));
        assertEquals(1, provider.exchanges.get());
    }

    @Test
    void malformedStoredStateFailsAsStorageError() throws Exception {
        connect(connections, key("tenant-a"), "account-a");
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT connection_id, revision FROM tpf_gmail_payloads")) {
            assertTrue(rows.next());
            String identity = rows.getString(1);
            long revision = rows.getLong(2);
            String malformed = encryption.encrypt(identity + ":" + revision,
                "{\"schemaVersion\":1,\"state\":null}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try (var update = connection.prepareStatement("UPDATE tpf_gmail_payloads SET encrypted_state=? WHERE connection_id=?")) {
                update.setString(1, malformed);
                update.setString(2, identity);
                update.executeUpdate();
            }
        }
        assertEquals(ConnectionFailure.Reason.STORAGE,
            assertThrows(ConnectionFailure.class, () -> store.read(key("tenant-a"))).reason());
    }

    @Test
    void abandonedReauthorizationRetainsTheExistingGrantButFreshAuthorizationExpires() {
        ConnectionKey existing = key("tenant-a");
        connect(connections, existing, "account-a");
        var grant = store.read(existing).orElseThrow().grant().orElseThrow();
        join(connections.begin(existing, "actor"));
        join(connections.begin(key("tenant-b"), "actor"));
        clock.advance(600_001);
        assertEquals(GmailConnections.Phase.READY, join(connections.status(existing)).phase());
        assertEquals(grant, store.read(existing).orElseThrow().grant().orElseThrow());
        assertTrue(store.read(existing).orElseThrow().challenge().isEmpty());
        join(connections.resolve(request("tenant-a")));
        assertEquals(GmailConnections.Phase.REQUIRES_REAUTHORIZATION,
            join(connections.status(key("tenant-b"))).phase());
    }

    @Test
    void durableStoreSurvivesNewManagerAndCoordinatesTwoHosts() throws Exception {
        connect(connections, key("tenant-a"), "account-a");
        clock.advance(3_550_000);
        provider.blockRefresh = true;
        GmailConnections second = manager(new JdbcGmailConnectionStore(database, encryption, "test-client-id"), provider);
        var firstCall = connections.resolve(request("tenant-a"));
        assertTrue(provider.entered.await(2, TimeUnit.SECONDS));
        var secondCall = second.resolve(request("tenant-a"));
        provider.release.countDown();
        join(firstCall);
        join(secondCall);
        assertEquals(1, provider.refreshes.get());
        assertEquals("rotated-refresh", store.read(key("tenant-a")).orElseThrow().grant().orElseThrow().refreshToken());
        GmailConnections restarted = manager(new JdbcGmailConnectionStore(database, encryption, "test-client-id"), provider);
        join(restarted.resolve(request("tenant-a")));
        assertEquals(1, provider.refreshes.get());
    }

    @Test
    void disconnectWinsAgainstInFlightRefreshAndBorrowedClientCannotStartAnotherRequest() throws Exception {
        connect(connections, key("tenant-a"), "account-a");
        var borrowed = join(connections.resolve(request("tenant-a")));
        clock.advance(3_550_000);
        provider.blockRefresh = true;
        var resolving = connections.resolve(request("tenant-a"));
        assertTrue(provider.entered.await(2, TimeUnit.SECONDS));
        join(connections.disconnect(key("tenant-a")));
        provider.release.countDown();
        assertThrows(CompletionException.class, () -> join(resolving));
        assertEquals(GmailConnections.Phase.DISCONNECTED, join(connections.status(key("tenant-a"))).phase());
        assertThrows(ConnectionFailure.class, () -> borrowed.client().users().messages().list("me").buildHttpRequest());
        assertTrue(store.read(key("tenant-a")).orElseThrow().grant().isEmpty());
    }

    @Test
    void reconnectCannotBeOverwrittenByAnOldRefresh() throws Exception {
        connect(connections, key("tenant-a"), "account-a");
        clock.advance(3_550_000);
        provider.blockRefresh = true;
        var old = connections.resolve(request("tenant-a"));
        assertTrue(provider.entered.await(2, TimeUnit.SECONDS));
        join(connections.disconnect(key("tenant-a")));
        connect(connections, key("tenant-a"), "account-new");
        provider.release.countDown();
        assertThrows(CompletionException.class, () -> join(old));
        assertEquals("account-new", store.read(key("tenant-a")).orElseThrow().grant().orElseThrow().subject());
    }

    @Test
    void crashOrLostRefreshResponseDoesNotRedispatchTheGrant() {
        connect(connections, key("tenant-a"), "account-a");
        clock.advance(3_550_000);
        provider.refreshFailure = Optional.of(ConnectionFailure.Reason.UNAVAILABLE);
        assertThrows(CompletionException.class, () -> join(connections.resolve(request("tenant-a"))));
        assertEquals(GmailConnections.Phase.REFRESHING, join(connections.status(key("tenant-a"))).phase());
        clock.advance(60_001);
        var restarted = manager(new JdbcGmailConnectionStore(database, encryption, "test-client-id"), provider);
        var failure = assertThrows(CompletionException.class, () -> join(restarted.resolve(request("tenant-a"))));
        assertEquals(ConnectionResolutionException.Kind.AUTHENTICATION_REQUIRED,
            ((ConnectionResolutionException) failure.getCause()).kind());
        assertEquals(1, provider.refreshes.get());
        assertTrue(store.read(key("tenant-a")).orElseThrow().grant().isEmpty());
    }

    @Test
    void definiteRateLimitAllowsLaterRetryAndTerminalGrantFailureRequiresConsent() {
        connect(connections, key("tenant-a"), "account-a");
        clock.advance(3_550_000);
        provider.refreshFailure = Optional.of(ConnectionFailure.Reason.RETRY_LATER);
        assertThrows(CompletionException.class, () -> join(connections.resolve(request("tenant-a"))));
        assertEquals(GmailConnections.Phase.READY, join(connections.status(key("tenant-a"))).phase());
        provider.refreshFailure = Optional.of(ConnectionFailure.Reason.REAUTHORIZE);
        assertThrows(CompletionException.class, () -> join(connections.resolve(request("tenant-a"))));
        assertEquals(GmailConnections.Phase.REQUIRES_REAUTHORIZATION, join(connections.status(key("tenant-a"))).phase());
    }

    @Test
    void storageEncryptsSecretsRemovesRetiredPayloadsAndRejectsWrongKeyOrTampering() throws Exception {
        connect(connections, key("tenant-a"), "account-a");
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT encrypted_state FROM tpf_gmail_payloads")) {
            assertTrue(rows.next());
            String encrypted = rows.getString(1);
            assertFalse(encrypted.contains("access-secret"));
            assertFalse(encrypted.contains("refresh-secret"));
            assertFalse(encrypted.contains("account-a"));
            assertFalse(rows.next(), "retired encrypted payloads must be removed");
        }
        var wrong = new ConnectionEncryption("other", Map.of("other", new SecretKeySpec(new byte[32], "AES")));
        assertThrows(ConnectionFailure.class, () -> new JdbcGmailConnectionStore(database, wrong, "test-client-id").read(key("tenant-a")));
        String sealed = encryption.encrypt("tenant-a:revision-1", "secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(ConnectionFailure.class, () -> encryption.decrypt("tenant-b:revision-1", sealed));
        join(connections.disconnect(key("tenant-a")));
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT COUNT(*) FROM tpf_gmail_revisions")) {
            rows.next();
            assertEquals(4, rows.getInt(1), "audit revisions survive credential deletion");
        }
    }

    @Test
    void reopeningAFileDatabasePreservesAuthorizationAndSupportsKeyRotation() throws Exception {
        JdbcDataSource fileDatabase = new JdbcDataSource();
        String url = "jdbc:h2:file:" + temporary.resolve("connections");
        fileDatabase.setURL(url);
        try (var connection = fileDatabase.getConnection(); var script = new InputStreamReader(
            getClass().getResourceAsStream("/META-INF/tpf-gmail-connections.sql"), java.nio.charset.StandardCharsets.UTF_8)) {
            RunScript.execute(connection, script);
        }
        var first = manager(new JdbcGmailConnectionStore(fileDatabase, encryption, "test-client-id"), provider);
        connect(first, key("tenant-a"), "account-a");
        first.close();
        JdbcDataSource reopened = new JdbcDataSource();
        reopened.setURL(url);
        byte[] secondKey = new byte[32];
        java.util.Arrays.fill(secondKey, (byte) 1);
        var rotated = new ConnectionEncryption("key-2", Map.of(
            "key-1", new SecretKeySpec(new byte[32], "AES"), "key-2", new SecretKeySpec(secondKey, "AES")));
        var nextProvider = new FakeAuthorization(clock);
        var second = manager(new JdbcGmailConnectionStore(reopened, rotated, "test-client-id"), nextProvider);
        join(second.resolve(request("tenant-a")));
        clock.advance(3_550_000);
        join(second.resolve(request("tenant-a")));
        try (var connection = reopened.getConnection(); var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT encrypted_state FROM tpf_gmail_payloads")) {
            assertTrue(result.next());
            assertTrue(result.getString(1).startsWith("key-2."));
        }
    }

    @Test
    void accountClaimsCommitWhenTheDataSourceDefaultsToManualTransactions() {
        JdbcDataSource manual = new JdbcDataSource();
        manual.setURL(database.getURL() + ";AUTOCOMMIT=OFF");
        var manualStore = new JdbcGmailConnectionStore(manual, encryption, "test-client-id");
        manualStore.claimAccount(key("tenant-a"), "account-a");
        manualStore.claimAccount(key("tenant-a"), "account-a");
        assertEquals(ConnectionFailure.Reason.FORBIDDEN,
            assertThrows(ConnectionFailure.class, () -> store.claimAccount(key("tenant-b"), "account-a")).reason());
        var first = new ConnectionState(1, GmailConnections.Phase.DISCONNECTED, clock.millis(), Optional.empty(), Optional.empty());
        assertTrue(manualStore.append(key("tenant-a"), first));
        assertFalse(manualStore.append(key("tenant-a"), first));
        assertEquals(1, store.read(key("tenant-a")).orElseThrow().revision());
    }

    @Test
    void competingRevisionHasOneWinnerAndDeletedPayloadIsStorageFailure() throws Exception {
        ConnectionKey key = key("tenant-a");
        var initial = new ConnectionState(1, GmailConnections.Phase.DISCONNECTED, clock.millis(), Optional.empty(), Optional.empty());
        assertTrue(store.append(key, initial));
        assertFalse(store.append(key, initial));
        try (var connection = database.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM tpf_gmail_payloads");
        }
        assertEquals(ConnectionFailure.Reason.STORAGE, assertThrows(ConnectionFailure.class, () -> store.read(key)).reason());
    }

    private GmailConnections manager(JdbcGmailConnectionStore store, FakeAuthorization authorization) {
        var manager = new GmailConnections(store, authorization, executor, clock);
        managers.add(manager);
        return manager;
    }

    private void connect(GmailConnections manager, ConnectionKey key, String subject) {
        var start = join(manager.begin(key, "actor"));
        join(manager.complete(key, "actor", state(start), start.browserSecret(), subject));
    }

    private String state(GmailConnections.AuthorizationStart start) { return start.redirect().getRawQuery().substring(6); }
    private ConnectionKey key(String tenant) { return new ConnectionKey(tenant, new ConnectionRef("gmail-main")); }
    private <T> T join(CompletionStage<T> stage) { return stage.toCompletableFuture().join(); }
    private ConnectionResolutionRequest<AuthenticatedGmailConnection> request(String tenant) {
        var context = new ConnectorExecutionContext(Optional.of(tenant), Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        return new ConnectionResolutionRequest<>(new ConnectionRef("gmail-main"), AuthenticatedGmailConnection.class, context);
    }

    static final class MutableClock extends Clock {
        private final AtomicLong time = new AtomicLong(1_800_000_000_000L);
        void advance(long millis) { time.addAndGet(millis); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(time.get()); }
    }

    static final class FakeAuthorization implements GmailAuthorization {
        final AtomicInteger exchanges = new AtomicInteger();
        final AtomicInteger refreshes = new AtomicInteger();
        final AtomicInteger clients = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean blockRefresh;
        volatile Optional<ConnectionFailure.Reason> refreshFailure = Optional.empty();
        private final Clock clock;
        FakeAuthorization(Clock clock) { this.clock = clock; }
        @Override public String registrationId() { return "test-client-id"; }
        @Override public URI authorize(String state, String verifier) { return URI.create("https://google.invalid/?state=" + state); }
        @Override public Grant exchange(String code, String verifier, Optional<Grant> previous) {
            exchanges.incrementAndGet();
            return grant(code, "refresh-secret");
        }
        @Override public Grant refresh(Grant previous) {
            refreshes.incrementAndGet();
            entered.countDown();
            if (blockRefresh) {
                try { assertTrue(release.await(3, TimeUnit.SECONDS)); } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new ConnectionFailure(ConnectionFailure.Reason.UNAVAILABLE);
                }
            }
            refreshFailure.ifPresent(reason -> { throw new ConnectionFailure(reason); });
            return grant(previous.subject(), "rotated-refresh");
        }
        private Grant grant(String subject, String refresh) {
            return new Grant(subject, "access-secret", refresh, clock.millis() + 3_600_000,
                Set.of(GmailQueryConnector.REQUIRED_OAUTH_SCOPE));
        }
        @Override public Gmail client(Grant grant, Runnable guard) {
            clients.incrementAndGet();
            return new Gmail.Builder(new MockHttpTransport(), GsonFactory.getDefaultInstance(), request -> guard.run())
                .setApplicationName("test").build();
        }
        @Override public void close() { }
    }
}
