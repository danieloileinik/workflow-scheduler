using OrderDemo.Contracts;

namespace OrderDemo.Orders;

public sealed class DemoOrder
{
    public Guid Id { get; init; }
    public decimal Amount { get; init; }
    public PaymentFailure PaymentFailure { get; init; }
    public Guid? ProcessId { get; set; }
    public DateTimeOffset CreatedAt { get; init; }
}
