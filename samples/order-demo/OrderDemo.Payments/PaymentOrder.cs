using OrderDemo.Contracts;

namespace OrderDemo.Payments;

public sealed class PaymentOrder(Guid id, decimal amount)
{
    public Guid Id { get; private set; } = id;
    public decimal Amount { get; private set; } = amount;
    public string State { get; private set; } = "NotCharged";

    public (bool Success, string Detail) Apply(OrderInput input, string command, int attempt)
    {
        if (input.Amount != Amount)
            return (false, "The payment amount cannot change for an existing order.");

        switch (command)
        {
            case DemoCommands.Refund:
                State = "Refunded";
                return (true, "Payment refunded; repeated refunds have no additional effect.");
            case DemoCommands.Charge when State == "Charged":
                return (true, "Payment was already charged; no additional charge was made.");
            case DemoCommands.Charge when State == "Refunded":
                return (false, "A refunded order cannot be charged again.");
            case DemoCommands.Charge when input.PaymentFailure == PaymentFailure.Always
                || input.PaymentFailure == PaymentFailure.Once && attempt == 0:
                return (false, "Simulated payment decline; no money was charged.");
            case DemoCommands.Charge:
                State = "Charged";
                return (true, $"Payment of {Amount:0.00} RUB charged.");
            default:
                return (false, "Unknown payment command.");
        }
    }
}
