
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    payload TEXT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    lease_start TIMESTAMP,
    lease_timeout INT,
    leased_to_worker TEXT,
    completed_at TIMESTAMP,
    retries INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    result TEXT,
    priority INT DEFAULT 5 CHECK (priority BETWEEN 1 AND 10)
);

CREATE INDEX IF NOT EXISTS idx_jobs_priority_status ON jobs (priority, status);

CREATE TABLE IF NOT EXISTS workers (
    id SERIAL PRIMARY KEY,
    state TEXT DEFAULT 'inactive',
    url TEXT UNIQUE,
    jobs_completed INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
