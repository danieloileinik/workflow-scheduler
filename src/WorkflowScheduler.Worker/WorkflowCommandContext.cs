using System.Globalization;
using WorkflowScheduler.Client;

namespace WorkflowScheduler.Worker;

public sealed record WorkflowCommandContext(
    StepExecution Execution,
    string Command,
    string CorrelationId,
    bool IsCompensation)
{
    public string IdempotencyKey =>
        $"{Execution.StepInstanceId:N}:{Execution.Attempt.ToString(CultureInfo.InvariantCulture)}";

    public string CallbackCorrelationId => $"worker:{IdempotencyKey}";
}