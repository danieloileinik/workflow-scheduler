using Microsoft.EntityFrameworkCore;
using OrderDemo.Contracts;
using WorkflowScheduler.Client;
using WorkflowScheduler.Worker;

namespace OrderDemo.Payments;

public partial class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddDbContext<PaymentDb>(options => options.UseNpgsql(builder.Configuration.GetConnectionString("Database")));
        builder.Services.AddWorkflowSchedulerClient(options => options.Address = new Uri(builder.Configuration["Scheduler:Address"]!));
        builder.Services.AddWorkflowSchedulerWorker(options =>
            {
                options.ServiceName = DemoCommands.Payments;
                options.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"]!;
            })
            .Handle<OrderInput, OperationResult, PaymentHandler>(DemoCommands.Charge)
            .Handle<OrderInput, OperationResult, PaymentHandler>(DemoCommands.Refund);

        var app = builder.Build();

        await using (var scope = app.Services.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<PaymentDb>().Database.EnsureCreatedAsync();

        app.MapGet("/health", async (PaymentDb db, CancellationToken ct) =>
            await db.Database.CanConnectAsync(ct) ? Results.Ok() : Results.StatusCode(503));
        app.MapGet("/orders/{id:guid}", async (Guid id, PaymentDb db, CancellationToken ct) =>
        {
            var order = await db.Orders.AsNoTracking().SingleOrDefaultAsync(value => value.Id == id, ct);
            var operations = await db.Executions.AsNoTracking().Where(value => value.OrderId == id)
                .OrderByDescending(value => value.RecordedAt).Take(50).ToListAsync(ct);

            return new ServiceSnapshot(order?.State ?? "NotCharged", operations);
        });
        await app.RunAsync();
    }
}
