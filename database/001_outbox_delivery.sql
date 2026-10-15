BEGIN;
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS "Attempts" integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "NextAttemptAt" timestamp with time zone,
    ADD COLUMN IF NOT EXISTS "FailedAt" timestamp with time zone,
    ADD COLUMN IF NOT EXISTS "ClaimId" uuid,
    ADD COLUMN IF NOT EXISTS "ClaimedUntil" timestamp with time zone;
CREATE INDEX IF NOT EXISTS "IX_outbox_messages_NextAttemptAt_CreatedAt_Id"
    ON outbox_messages ("NextAttemptAt", "CreatedAt", "Id")
    WHERE "ProcessedAt" IS NULL AND "FailedAt" IS NULL;
CREATE INDEX IF NOT EXISTS "IX_outbox_messages_ClaimId"
    ON outbox_messages ("ClaimId") WHERE "ClaimId" IS NOT NULL;
DROP INDEX IF EXISTS "IX_outbox_messages_ProcessedAt";
COMMIT;
