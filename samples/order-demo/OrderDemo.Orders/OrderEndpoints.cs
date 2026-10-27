using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;
using WorkflowScheduler.Client;

namespace OrderDemo.Orders;

public static class OrderEndpoints
{
    public static void MapOrders(this WebApplication app)
    {
        app.MapGet("/api/orders", async (OrderDb db, CancellationToken ct) =>
            await db.Orders.AsNoTracking().OrderByDescending(order => order.CreatedAt).Take(50).ToListAsync(ct));
        app.MapPost("/api/orders", CreateAsync);
        app.MapGet("/api/orders/{id:guid}", async (Guid id, OrderDb db, OrderDetailsReader reader, CancellationToken ct) =>
        {
            var order = await db.Orders.AsNoTracking().SingleOrDefaultAsync(order => order.Id == id, ct);

            return order is null ? Results.NotFound() : Results.Ok(await reader.ReadAsync(order, ct));
        });
        app.MapPost("/api/orders/{id:guid}/approve", async (Guid id, OrderDb db,
            IWorkflowSchedulerClient scheduler, CancellationToken ct) =>
        {
            var order = await db.Orders.FindAsync([id], ct);
            if (order?.ProcessId is not Guid processId)
                return Results.NotFound();

            await scheduler.SendEventAsync(processId, DemoCommands.Approval, new { Approved = true }, $"approve:{id:N}", ct);

            return Results.NoContent();
        });
        app.MapPost("/api/orders/{id:guid}/cancel", async (Guid id, OrderDb db,
            IWorkflowSchedulerClient scheduler, CancellationToken ct) =>
        {
            var order = await db.Orders.FindAsync([id], ct);
            if (order?.ProcessId is not Guid processId)
                return Results.NotFound();

            var process = await scheduler.GetStatusPageAsync(processId, cancellationToken: ct);
            var current = process.StepHistory.FirstOrDefault(step => step.Id == process.CurrentStepId);
            if (process.Status != WorkflowProcessStatus.Waiting || current?.Status != WorkflowStepStatus.Waiting)
                return Results.Conflict(new { detail = "Для демонстрации компенсации дождитесь подтверждения заказа." });

            await scheduler.CancelAsync(processId, ct);

            return Results.NoContent();
        });
    }

    private static async Task<IResult> CreateAsync(OrderInput input, OrderDb db, OrderWorkflow workflow,
        IWorkflowSchedulerClient scheduler, CancellationToken ct)
    {
        if (input.OrderId == Guid.Empty || input.Amount is <= 0 or > 1_000_000
            || decimal.Round(input.Amount, 2) != input.Amount || !Enum.IsDefined(input.PaymentFailure))
            return Results.BadRequest(new { detail = "Нужны UUID заказа, сумма от 0.01 до 1000000 и допустимый сценарий." });

        var failure = input.PaymentFailure.ToString();
        var createdAt = DateTimeOffset.UtcNow;
        await db.Database.ExecuteSqlInterpolatedAsync($"""
            INSERT INTO orders ("Id", "Amount", "PaymentFailure", "CreatedAt")
            VALUES ({input.OrderId}, {input.Amount}, {failure}, {createdAt}) ON CONFLICT ("Id") DO NOTHING
            """, ct);

        var order = await db.Orders.AsNoTracking().SingleAsync(value => value.Id == input.OrderId, ct);
        if (order.Amount != input.Amount || order.PaymentFailure != input.PaymentFailure)
            return Results.Conflict(new { detail = "Этот UUID уже использован для другого заказа." });
        if (order.ProcessId is null)
        {
            var definition = await workflow.GetOrRegisterAsync(ct);
            // Persisted decimal values keep request serialization stable when recovering a lost start response.
            var persistedInput = new OrderInput(order.Id, order.Amount, order.PaymentFailure);
            var processId = await scheduler.StartAsync(definition, persistedInput, $"order:{order.Id:N}", order.Id, ct);
            await db.Orders.Where(value => value.Id == input.OrderId)
                .ExecuteUpdateAsync(update => update.SetProperty(value => value.ProcessId, processId), ct);
            order.ProcessId = processId;
        }

        return Results.Created($"/api/orders/{order.Id}", order);
    }
}
