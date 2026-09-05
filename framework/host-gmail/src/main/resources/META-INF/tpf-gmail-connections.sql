-- Apply explicitly to a dedicated host database. PostgreSQL and H2 compatible.
-- Audit revisions contain no credentials; old encrypted payloads are removed on transition.
CREATE TABLE tpf_gmail_revisions (
    connection_id VARCHAR(64) NOT NULL,
    revision BIGINT NOT NULL,
    phase VARCHAR(32) NOT NULL,
    changed_at BIGINT NOT NULL,
    PRIMARY KEY (connection_id, revision)
);
CREATE TABLE tpf_gmail_payloads (
    connection_id VARCHAR(64) NOT NULL,
    revision BIGINT NOT NULL,
    encrypted_state VARCHAR(32768) NOT NULL,
    PRIMARY KEY (connection_id, revision),
    FOREIGN KEY (connection_id, revision) REFERENCES tpf_gmail_revisions(connection_id, revision)
);
-- One logical owner per Google subject and OAuth client registration, including after disconnect.
-- Ownership transfer and aliasing need an explicit administrative migration, not another consent flow.
CREATE TABLE tpf_gmail_accounts (
    account_id VARCHAR(64) PRIMARY KEY,
    connection_id VARCHAR(64) NOT NULL
);
