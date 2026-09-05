package org.pipelineframework.host.gmail;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/** Provisional host storage implementation; SQL runs only on the host's blocking executor. */
public final class JdbcGmailConnectionStore {
    private final DataSource dataSource;
    private final ConnectionEncryption encryption;
    private final String registration;
    private final ObjectMapper json = new ObjectMapper().registerModule(new Jdk8Module());
    private record Stored(int schemaVersion, ConnectionState state) { }

    /** registration must identify the actual Google OAuth client ID, consistently across all hosts. */
    public JdbcGmailConnectionStore(DataSource dataSource, ConnectionEncryption encryption, String registration) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.encryption = Objects.requireNonNull(encryption);
        this.registration = Objects.requireNonNull(registration);
        if (registration.isBlank()) { throw new IllegalArgumentException("Registration required"); }
    }

    ConnectionEncryption encryption() { return encryption; }

    String registrationId() { return registration; }

    private String identity(ConnectionKey key) {
        return encryption.hash(registration.length() + ":" + registration + key.tenant().length() + ":"
            + key.tenant() + key.reference().value());
    }

    Optional<ConnectionState> read(ConnectionKey key) {
        String id = identity(key);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
            "SELECT r.revision, p.encrypted_state FROM tpf_gmail_revisions r LEFT JOIN tpf_gmail_payloads p "
                + "ON r.connection_id=p.connection_id AND r.revision=p.revision WHERE r.connection_id=? "
                + "AND r.revision=(SELECT MAX(v.revision) FROM tpf_gmail_revisions v WHERE v.connection_id=?)")) {
            statement.setString(1, id);
            statement.setString(2, id);
            try (var result = statement.executeQuery()) {
                if (!result.next()) { return Optional.empty(); }
                long revision = result.getLong(1);
                String payload = Optional.ofNullable(result.getString(2))
                    .orElseThrow(() -> new ConnectionFailure(ConnectionFailure.Reason.STORAGE));
                Stored stored = json.readValue(encryption.decrypt(id + ":" + revision, payload), Stored.class);
                if (stored.schemaVersion() != 1) { throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE); }
                ConnectionState state = Objects.requireNonNull(stored.state(), "Connection state required");
                if (state.revision() != revision) { throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE); }
                return Optional.of(state);
            }
        } catch (SQLException | java.io.IOException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }

    boolean append(ConnectionKey key, ConnectionState state) {
        String id = identity(key);
        try (var connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (var insert = connection.prepareStatement(
                    "INSERT INTO tpf_gmail_revisions(connection_id,revision,phase,changed_at) VALUES(?,?,?,?)")) {
                    insert.setString(1, id);
                    insert.setLong(2, state.revision());
                    insert.setString(3, state.phase().name());
                    insert.setLong(4, state.changedAt());
                    insert.executeUpdate();
                }
                String payload = encryption.encrypt(id + ":" + state.revision(), json.writeValueAsBytes(new Stored(1, state)));
                try (var insert = connection.prepareStatement(
                    "INSERT INTO tpf_gmail_payloads(connection_id,revision,encrypted_state) VALUES(?,?,?)")) {
                    insert.setString(1, id);
                    insert.setLong(2, state.revision());
                    insert.setString(3, payload);
                    insert.executeUpdate();
                }
                try (var delete = connection.prepareStatement(
                    "DELETE FROM tpf_gmail_payloads WHERE connection_id=? AND revision<?")) {
                    delete.setString(1, id);
                    delete.setLong(2, state.revision());
                    delete.executeUpdate();
                }
                connection.commit();
                return true;
            } catch (SQLException failure) {
                connection.rollback();
                if ("23505".equals(failure.getSQLState())) { return false; }
                throw failure;
            } catch (java.io.IOException | RuntimeException failure) {
                connection.rollback();
                throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }

    /** The unique claim survives disconnect; aliases cannot create independent refresh authorities. */
    void claimAccount(ConnectionKey key, String subject) {
        String account = encryption.hash(registration.length() + ":" + registration + subject);
        String id = identity(key);
        try (var connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (var insert = connection.prepareStatement(
                    "INSERT INTO tpf_gmail_accounts(account_id,connection_id) VALUES(?,?)")) {
                    insert.setString(1, account);
                    insert.setString(2, id);
                    insert.executeUpdate();
                } catch (SQLException duplicate) {
                    connection.rollback();
                    if (!"23505".equals(duplicate.getSQLState())) { throw duplicate; }
                    try (var select = connection.prepareStatement(
                        "SELECT connection_id FROM tpf_gmail_accounts WHERE account_id=?")) {
                        select.setString(1, account);
                        try (var result = select.executeQuery()) {
                            if (!result.next() || !id.equals(result.getString(1))) {
                                throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
                            }
                        }
                    }
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }
}
