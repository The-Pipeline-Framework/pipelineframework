package org.pipelineframework.host.oidc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectionRef;

class JdbcConnectionStoreTest {
    @TempDir Path temporary;
    private final ConnectionKey key = new ConnectionKey("tenant-a", new ConnectionRef("main"));
    private final ConnectionEncryption encryption = new ConnectionEncryption("first",
        Map.of("first", new SecretKeySpec(new byte[32], "AES")));
    private JdbcDataSource database;
    private JdbcConnectionStore store;

    @BeforeEach void setup() throws Exception {
        database = database("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        schema(database);
        store = new JdbcConnectionStore(database, encryption, "issuer|client");
    }

    @Test void encryptionBindsAuthorityAndRetiredSecretsAreDeleted() throws Exception {
        var ready = ready();
        assertTrue(store.append(key, ready));
        assertTrue(store.append(key, ready.next(QuarkusConnections.Phase.READY, 2, ready.grant(), Optional.empty())));
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT encrypted_state FROM tpf_oidc_payloads")) {
            assertTrue(rows.next());
            String sealed = rows.getString(1);
            for (String secret : Set.of("access-secret", "refresh-secret", "account-secret")) {
                assertFalse(sealed.contains(secret));
                assertFalse(ready.toString().contains(secret));
                assertFalse(ready.grant().orElseThrow().toString().contains(secret));
            }
            assertFalse(rows.next());
        }
        String sealed = encryption.encrypt("tenant-a:1", "secret".getBytes(StandardCharsets.UTF_8));
        assertThrows(ConnectionFailure.class, () -> encryption.decrypt("tenant-b:1", sealed));
        assertThrows(ConnectionFailure.class, () -> encryption.decrypt("tenant-a:2", sealed));
        var unavailable = new ConnectionEncryption("other", Map.of("other", new SecretKeySpec(new byte[32], "AES")));
        assertThrows(ConnectionFailure.class, () -> new JdbcConnectionStore(database, unavailable, "issuer|client").read(key));
        var current = store.read(key).orElseThrow();
        assertTrue(store.append(key, current.replace(QuarkusConnections.Phase.DISCONNECTED, 3, Optional.empty(), Optional.empty())));
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT COUNT(*) FROM tpf_oidc_revisions")) {
            assertTrue(rows.next()); assertEquals(3, rows.getInt(1));
        }
        assertTrue(store.read(key).orElseThrow().grant().isEmpty());
    }

    @Test void fileDatabaseReopensAndRotatesEncryptionKeys() throws Exception {
        String url = "jdbc:h2:file:" + temporary.resolve("connections");
        var file = database(url);
        schema(file);
        var first = new JdbcConnectionStore(file, encryption, "issuer|client");
        assertTrue(first.append(key, ready()));
        // H2 closes the file when the last connection closes; no manager or open JDBC connection survives.
        byte[] nextKey = new byte[32]; java.util.Arrays.fill(nextKey, (byte) 1);
        var rotated = new ConnectionEncryption("second", Map.of("first", new SecretKeySpec(new byte[32], "AES"),
            "second", new SecretKeySpec(nextKey, "AES")));
        var reopened = database(url);
        var next = new JdbcConnectionStore(reopened, rotated, "issuer|client");
        var loaded = next.read(key).orElseThrow();
        assertEquals("refresh-secret", loaded.grant().orElseThrow().refreshToken());
        assertTrue(next.append(key, loaded.next(QuarkusConnections.Phase.READY, 2, loaded.grant(), Optional.empty())));
        try (var connection = reopened.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT encrypted_state FROM tpf_oidc_payloads")) {
            assertTrue(rows.next()); assertTrue(rows.getString(1).startsWith("second."));
        }
        assertThrows(ConnectionFailure.class, () -> first.read(key));
    }

    @Test void manualTransactionsCommitClaimsAndConditionalRevisions() {
        var manual = new JdbcConnectionStore(database(database.getURL() + ";AUTOCOMMIT=OFF"), encryption, "issuer|client");
        manual.claimAccount(key, "account"); manual.claimAccount(key, "account");
        assertEquals(ConnectionFailure.Reason.FORBIDDEN, assertThrows(ConnectionFailure.class,
            () -> store.claimAccount(new ConnectionKey("tenant-b", key.reference()), "account")).reason());
        assertTrue(manual.append(key, ready()));
        assertFalse(store.append(key, ready()));
        assertEquals(1, store.read(key).orElseThrow().revision());
    }

    @Test void missingOrMalformedLatestPayloadFailsClosed() throws Exception {
        assertTrue(store.append(key, ready()));
        try (var connection = database.getConnection(); var query = connection.createStatement();
             var rows = query.executeQuery("SELECT connection_id, revision FROM tpf_oidc_payloads")) {
            assertTrue(rows.next());
            String id = rows.getString(1);
            String corrupt = encryption.encrypt(id + ":" + rows.getLong(2),
                "{\"schemaVersion\":1,\"state\":null}".getBytes(StandardCharsets.UTF_8));
            try (var update = connection.prepareStatement("UPDATE tpf_oidc_payloads SET encrypted_state=? WHERE connection_id=?")) {
                update.setString(1, corrupt); update.setString(2, id); update.executeUpdate();
            }
        }
        assertEquals(ConnectionFailure.Reason.STORAGE, assertThrows(ConnectionFailure.class, () -> store.read(key)).reason());
        try (var connection = database.getConnection(); var query = connection.createStatement()) {
            query.executeUpdate("DELETE FROM tpf_oidc_payloads");
        }
        assertEquals(ConnectionFailure.Reason.STORAGE, assertThrows(ConnectionFailure.class, () -> store.read(key)).reason());
    }

    private ConnectionState ready() {
        return new ConnectionState(1, 1, QuarkusConnections.Phase.READY, 1,
            Optional.of(new ConnectionState.Grant("account-secret", "access-secret", "refresh-secret", 10000, Set.of("read"))), Optional.empty());
    }
    private JdbcDataSource database(String url) { var result = new JdbcDataSource(); result.setURL(url); return result; }
    private void schema(JdbcDataSource source) throws Exception {
        try (var connection = source.getConnection(); var script = new InputStreamReader(
            getClass().getResourceAsStream("/META-INF/tpf-oidc-connections.sql"), StandardCharsets.UTF_8)) {
            RunScript.execute(connection, script);
        }
    }
}
