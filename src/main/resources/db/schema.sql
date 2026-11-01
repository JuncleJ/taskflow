CREATE TABLE IF NOT EXISTS users
(
    id
    UUID
    PRIMARY
    KEY,
    login
    VARCHAR
(
    64
) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL
    );
CREATE TABLE IF NOT EXISTS sessions
(
    token_hash
    VARCHAR
(
    64
) PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users
(
    id
),
    expires_at TIMESTAMPTZ NOT NULL
    );
CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
CREATE TABLE IF NOT EXISTS tasks
(
    id
    UUID
    PRIMARY
    KEY,
    owner_id
    UUID
    NOT
    NULL
    REFERENCES
    users
(
    id
),
    type VARCHAR
(
    64
) NOT NULL,
    status VARCHAR
(
    16
) NOT NULL CHECK
(
    status
    IN
(
    'PENDING',
    'RUNNING',
    'COMPLETED',
    'FAILED'
)),
    progress INTEGER NOT NULL DEFAULT 0 CHECK
(
    progress
    BETWEEN
    0
    AND
    100
),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error TEXT
    );
CREATE INDEX IF NOT EXISTS tasks_owner_created ON tasks(owner_id, created_at DESC);
