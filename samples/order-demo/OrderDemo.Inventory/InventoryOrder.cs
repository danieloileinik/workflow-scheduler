using OrderDemo.Contracts;

namespace OrderDemo.Inventory;

public sealed class InventoryOrder(Guid id)
{
    public Guid Id { get; private set; } = id;
    public string State { get; private set; } = "NotReserved";

    public (bool Success, string Detail) Apply(string command)
    {
        switch (command)
        {
            case DemoCommands.Reserve when State == "Released":
                return (false, "A released reservation cannot be reopened.");
            case DemoCommands.Reserve when State is "Reserved" or "Shipped":
                return (true, "Stock was already reserved; no additional reservation was made.");
            case DemoCommands.Reserve:
                State = "Reserved";
                return (true, "Stock reserved.");
            case DemoCommands.Release when State == "Shipped":
                return (false, "Shipped stock cannot be released.");
            case DemoCommands.Release:
                State = "Released";
                return (true, "Stock reservation released.");
            case DemoCommands.Ship when State == "Shipped":
                return (true, "Order was already shipped; no additional shipment was made.");
            case DemoCommands.Ship when State == "Reserved":
                State = "Shipped";
                return (true, "Order shipped.");
            case DemoCommands.Ship:
                return (false, "Shipping requires an active stock reservation.");
            default:
                return (false, "Unknown inventory command.");
        }
    }
}
