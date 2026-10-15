using Grpc.Core;
using OrderDemo.Contracts;
using WorkflowScheduler.Client;

namespace OrderDemo.Orders;

public sealed class OrderWorkflow(IWorkflowSchedulerClient scheduler)
{
    public static WorkflowDefinition Definition { get; } = Workflow.Define("samples.order-demo", 1)
        .Call(DemoCommands.Inventory, DemoCommands.Reserve, "Резерв товара")
        .CompensateWith(DemoCommands.Inventory, DemoCommands.Release, "Освобождение резерва")
        .Call(DemoCommands.Payments, DemoCommands.Charge, "Оплата")
        .CompensateWith(DemoCommands.Payments, DemoCommands.Refund, "Возврат оплаты")
        .WaitFor(DemoCommands.Approval, "Подтверждение заказа")
        .Call(DemoCommands.Inventory, DemoCommands.Ship, "Отгрузка")
        .Build();

    public async Task<RegisteredWorkflow> GetOrRegisterAsync(CancellationToken ct)
    {
        try
        {
            return await scheduler.GetDefinitionAsync(Definition.Name, Definition.Version, ct);
        }
        catch (WorkflowClientException exception) when (exception.StatusCode == StatusCode.NotFound)
        {
            try
            {
                return await scheduler.RegisterAsync(Definition, ct);
            }
            catch (WorkflowClientException conflict) when (conflict.StatusCode == StatusCode.FailedPrecondition)
            {
                return await scheduler.GetDefinitionAsync(Definition.Name, Definition.Version, ct);
            }
        }
    }
}
