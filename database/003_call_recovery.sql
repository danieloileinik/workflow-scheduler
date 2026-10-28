BEGIN;
LOCK TABLE step_instances IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'step_instances' AND column_name = 'NextRecoveryAt'
    ) THEN
        ALTER TABLE step_instances ADD COLUMN "NextRecoveryAt" timestamp with time zone;

        -- Give existing running calls a deadline once; pending outbox deliveries remain idempotent.
        UPDATE step_instances
        SET "NextRecoveryAt" = CURRENT_TIMESTAMP + INTERVAL '5 minutes'
        WHERE "Status" = 'Running';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS "IX_step_instances_NextRecoveryAt"
    ON step_instances ("NextRecoveryAt") WHERE "NextRecoveryAt" IS NOT NULL;
COMMIT;
