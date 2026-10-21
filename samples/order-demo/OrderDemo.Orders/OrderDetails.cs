using OrderDemo.Contracts;
using WorkflowScheduler.Client;

namespace OrderDemo.Orders;

public sealed record OrderDetails(
    DemoOrder Order,
    WorkflowProcessSnapshot? Process,
    ServiceSnapshot? Inventory,
    ServiceSnapshot? Payment,
    IReadOnlyList<string> Warnings);
