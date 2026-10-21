namespace OrderDemo.Contracts;

public sealed record OrderInput(Guid OrderId, decimal Amount, PaymentFailure PaymentFailure);
