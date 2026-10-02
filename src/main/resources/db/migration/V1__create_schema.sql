-- Core schema for the durable job queue.
-- Convention: higher `priority` value = claimed first.

CREATE TABLE jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name        TEXT        NOT NULL DEFAULT 'default',
    type              TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT        NOT NULL DEFAULT 'PENDING',
    priority          INT         NOT NULL DEFAULT 0,
    attempts          INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 5,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by         TEXT,
    lease_expires_at  TIMESTAMPTZ,
    last_error        TEXT,
    idempotency_key   TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,

    CONSTRAINT jobs_status_chk
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD', 'CANCELLED')),
    CONSTRAINT jobs_attempts_chk CHECK (attempts >= 0 AND max_attempts >= 1),
    -- A RUNNING job must always have an owner and a lease; nothing else may hold one.
    CONSTRAINT jobs_lease_chk CHECK (
        (status = 'RUNNING' AND locked_by IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'RUNNING' AND locked_by IS NULL AND lease_expires_at IS NULL)
    )
);

-- Idempotency: Postgres treats NULLs as distinct in unique indexes, so keyless jobs never collide.
CREATE UNIQUE INDEX uq_jobs_idempotency_key ON jobs (idempotency_key);

-- Claim query: WHERE status='PENDING' AND run_at <= now() ORDER BY priority DESC, run_at.
-- Partial index = only runnable rows, so it stays small however many finished jobs pile up.
CREATE INDEX idx_jobs_claim ON jobs (priority DESC, run_at) WHERE status = 'PENDING';

-- Reaper query: RUNNING jobs whose lease has expired.
CREATE INDEX idx_jobs_lease ON jobs (lease_expires_at) WHERE status = 'RUNNING';

-- Listing/filtering endpoint (GET /jobs?status=&type=&queue=) and stats.
CREATE INDEX idx_jobs_status_created ON jobs (status, created_at DESC);
CREATE INDEX idx_jobs_queue_type ON jobs (queue_name, type);

CREATE TABLE job_attempts (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id          UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_number  INT         NOT NULL,
    worker_id       TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    outcome         TEXT,
    error_message   TEXT,
    stack_trace     TEXT,

    CONSTRAINT job_attempts_outcome_chk
        CHECK (outcome IS NULL OR outcome IN
               ('SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_NON_RETRYABLE', 'TIMED_OUT', 'LEASE_EXPIRED', 'RELEASED')),
    -- One row per (job, attempt number): also guards against a zombie double-recording an attempt.
    CONSTRAINT uq_job_attempts_job_attempt UNIQUE (job_id, attempt_number)
);

-- Copy of the job at time of death. Deliberately NO foreign key to jobs: the DLQ record must
-- survive later cleanup of old job rows.
CREATE TABLE dead_letter_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id           UUID        NOT NULL,
    queue_name       TEXT        NOT NULL,
    type             TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    priority         INT         NOT NULL,
    attempts         INT         NOT NULL,
    max_attempts     INT         NOT NULL,
    last_error       TEXT,
    reason           TEXT        NOT NULL,
    job_created_at   TIMESTAMPTZ NOT NULL,
    dead_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at      TIMESTAMPTZ,
    replayed_job_id  UUID
);

CREATE INDEX idx_dlq_job_id ON dead_letter_jobs (job_id);
CREATE INDEX idx_dlq_dead_at ON dead_letter_jobs (dead_at DESC);
CREATE INDEX idx_dlq_unreplayed ON dead_letter_jobs (dead_at) WHERE replayed_at IS NULL;
