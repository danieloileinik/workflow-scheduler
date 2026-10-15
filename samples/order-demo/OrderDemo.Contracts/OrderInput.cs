namespace OrderDemo.Contracts;

public enum PaymentFailure
{
    None,
    Once,
    Always
}

public sealed record OrderInput(Guid OrderId, decimal Amount, PaymentFailure PaymentFailure);

public sealed record OperationResult(string Detail);

public sealed record OperationReceipt(
    string ExecutionKey,
    Guid OrderId,
    Guid StepInstanceId,
    int Attempt,
    string Command,
    bool Success,
    string Detail,
    DateTimeOffset RecordedAt);

public sealed record ServiceSnapshot(string State, IReadOnlyList<OperationReceipt> Operations);

public static class DemoCommands
{
    public const string Inventory = "demo-inventory";
    public const string Payments = "demo-payments";
    public const string Reserve = "ReserveStock";
    public const string Release = "ReleaseStock";
    public const string Ship = "ShipOrder";
    public const string Charge = "ChargePayment";
    public const string Refund = "RefundPayment";
    public const string Approval = "OrderApproved";
}
