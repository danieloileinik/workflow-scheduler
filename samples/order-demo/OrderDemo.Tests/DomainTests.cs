using OrderDemo.Contracts;
using OrderDemo.Inventory;
using OrderDemo.Payments;
using Xunit;

namespace OrderDemo.Tests;

[Trait("Category", "Unit")]
public class DomainTests
{
    [Fact]
    public void ReleasedReservation_CannotBeReopenedOrShipped()
    {
        var order = new InventoryOrder(Guid.NewGuid());
        Assert.True(order.Apply(DemoCommands.Reserve).Success);
        Assert.True(order.Apply(DemoCommands.Release).Success);
        Assert.True(order.Apply(DemoCommands.Release).Success);

        Assert.False(order.Apply(DemoCommands.Reserve).Success);
        Assert.False(order.Apply(DemoCommands.Ship).Success);
        Assert.Equal("Released", order.State);
    }

    [Fact]
    public void Shipment_RequiresReservationAndCannotBeReleased()
    {
        var order = new InventoryOrder(Guid.NewGuid());
        Assert.False(order.Apply(DemoCommands.Ship).Success);
        Assert.True(order.Apply(DemoCommands.Reserve).Success);
        Assert.True(order.Apply(DemoCommands.Ship).Success);
        Assert.True(order.Apply(DemoCommands.Ship).Success);

        Assert.False(order.Apply(DemoCommands.Release).Success);
        Assert.Equal("Shipped", order.State);
    }

    [Theory]
    [InlineData(PaymentFailure.None, 0, true)]
    [InlineData(PaymentFailure.Once, 0, false)]
    [InlineData(PaymentFailure.Once, 1, true)]
    [InlineData(PaymentFailure.Always, 0, false)]
    [InlineData(PaymentFailure.Always, 3, false)]
    public void PaymentScenario_FailsBeforeChargingMoney(PaymentFailure failure, int attempt, bool success)
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, failure);
        var order = new PaymentOrder(input.OrderId, input.Amount);

        Assert.Equal(success, order.Apply(input, DemoCommands.Charge, attempt).Success);
        Assert.Equal(success ? "Charged" : "NotCharged", order.State);
    }

    [Fact]
    public void RefundedPayment_CannotBeChargedAgainOrChangeItsAmount()
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, PaymentFailure.None);
        var order = new PaymentOrder(input.OrderId, input.Amount);
        Assert.True(order.Apply(input, DemoCommands.Charge, 0).Success);
        Assert.False(order.Apply(input with { Amount = 1 }, DemoCommands.Charge, 1).Success);
        Assert.True(order.Apply(input, DemoCommands.Refund, 0).Success);
        Assert.True(order.Apply(input, DemoCommands.Refund, 1).Success);

        Assert.False(order.Apply(input, DemoCommands.Charge, 2).Success);
        Assert.Equal("Refunded", order.State);
    }
}
