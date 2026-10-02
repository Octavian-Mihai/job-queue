-- Side-effect ledger for the idempotent send-email demo handler.
-- Delivery is at-least-once, so the handler dedupes its own side effect on a stable key (job id).
CREATE TABLE email_outbox (
    dedupe_key  TEXT        PRIMARY KEY,
    recipient   TEXT        NOT NULL,
    subject     TEXT,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
