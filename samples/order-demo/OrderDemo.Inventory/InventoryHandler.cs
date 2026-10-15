using System.Buffers.Binary;
using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;
using WorkflowScheduler.Worker;

namespace OrderDemo.Inventory;

public sealed class InventoryHandler(InventoryDb db) : IWorkflowCommandHandler<OrderInput, OperationResult>
{
    public async Task<OperationResult> HandleAsync(OrderInput input, WorkflowCommandContext context, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var lockId = BinaryPrimitives.ReadInt64BigEndian(input.OrderId.ToByteArray());
        await db.Database.ExecuteSqlInterpolatedAsync($"SELECT pg_advisory_xact_lock({lockId})", ct);
        var receipt = await db.Executions.FindAsync([context.IdempotencyKey], ct);
        if (receipt is null)
        {
            var order = await db.Orders.FindAsync([input.OrderId], ct);
            if (order is null)
            {
                order = new InventoryOrder(input.OrderId);
                db.Orders.Add(order);
            }

            var outcome = order.Apply(context.Command);
            receipt = new OperationReceipt(context.IdempotencyKey, input.OrderId, context.Execution.StepInstanceId,
                context.Execution.Attempt, context.Command, outcome.Success, outcome.Detail, DateTimeOffset.UtcNow);
            db.Executions.Add(receipt);
            await db.SaveChangesAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return receipt.Success ? new OperationResult(receipt.Detail) : throw new InvalidOperationException(receipt.Detail);
    }
}
