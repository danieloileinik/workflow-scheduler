namespace OrderDemo.Contracts;

public sealed record ServiceSnapshot(string State, IReadOnlyList<OperationReceipt> Operations);
