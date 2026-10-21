CREATE INDEX IF NOT EXISTS "IX_step_instances_ProcessInstanceId_Sequence"
    ON step_instances ("ProcessInstanceId", "Sequence");

CREATE INDEX IF NOT EXISTS "IX_saga_compensation_logs_ProcessInstanceId_Status_Sequence"
    ON saga_compensation_logs ("ProcessInstanceId", "Status", "Sequence");
