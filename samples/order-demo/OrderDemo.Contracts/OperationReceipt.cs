namespace OrderDemo.Contracts;

public sealed record OperationReceipt(
    string ExecutionKey,
    Guid OrderId,
    Guid StepInstanceId,
    int Attempt,
    string Command,
    bool Success,
    string Detail,
    DateTimeOffset RecordedAt);
