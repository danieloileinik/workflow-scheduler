using Microsoft.EntityFrameworkCore;
using Npgsql;
using OrderDemo.Contracts;
using OrderDemo.Inventory;
using OrderDemo.Payments;
using Testcontainers.PostgreSql;
using WorkflowScheduler.Client;
using WorkflowScheduler.Worker;
using Xunit;

namespace OrderDemo.Tests;

public sealed class StoreFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public InventoryDb Inventory() => new(new DbContextOptionsBuilder<InventoryDb>().UseNpgsql(Connection("inventory")).Options);
    public PaymentDb Payments() => new(new DbContextOptionsBuilder<PaymentDb>().UseNpgsql(Connection("payments")).Options);

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
        await connection.OpenAsync();
        foreach (var database in new[] { "inventory", "payments" })
        {
            await using var command = new NpgsqlCommand($"CREATE DATABASE {database}", connection);
            await command.ExecuteNonQueryAsync();
        }
        await using var inventory = Inventory();
        await using var payments = Payments();
        await inventory.Database.EnsureCreatedAsync();
        await payments.Database.EnsureCreatedAsync();
    }

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    private string Connection(string database) =>
        new NpgsqlConnectionStringBuilder(_postgres.GetConnectionString()) { Database = database }.ConnectionString;
}

[Trait("Category", "Integration")]
public class StoreTests(StoreFixture fixture) : IClassFixture<StoreFixture>
{
    [Fact]
    public async Task ConcurrentDuplicateCharges_CommitOneReceiptAndReuseTheResult()
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, PaymentFailure.None);
        var context = Context(DemoCommands.Charge);

        var results = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => Charge(input, context)));

        Assert.Single(results.Distinct());
        await using var db = fixture.Payments();
        Assert.Equal("Charged", (await db.Orders.SingleAsync(order => order.Id == input.OrderId)).State);
        Assert.Equal(1, await db.Executions.CountAsync(receipt => receipt.OrderId == input.OrderId));
    }

    [Fact]
    public async Task FailedReceipt_IsReplayedAndTheNextAttemptCanSucceed()
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, PaymentFailure.Once);
        var context = Context(DemoCommands.Charge);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Charge(input, context));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Charge(input, context));

        await Charge(input, context with { Execution = context.Execution with { Attempt = 1 } });

        await using var db = fixture.Payments();
        var receipts = await db.Executions.Where(receipt => receipt.OrderId == input.OrderId).OrderBy(receipt => receipt.Attempt).ToListAsync();
        Assert.Equal(new[] { false, true }, receipts.Select(receipt => receipt.Success));
        Assert.Equal("Charged", (await db.Orders.SingleAsync(order => order.Id == input.OrderId)).State);
    }

    [Fact]
    public async Task Refund_IsDurableAndLateChargesCannotReopenIt()
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, PaymentFailure.None);
        var charge = Context(DemoCommands.Charge);
        await Charge(input, charge);
        var refund = Context(DemoCommands.Refund);
        await Charge(input, refund);
        await Charge(input, refund);

        await Assert.ThrowsAsync<InvalidOperationException>(() => Charge(input,
            charge with { Execution = charge.Execution with { Attempt = 1 } }));

        await using var db = fixture.Payments();
        Assert.Equal("Refunded", (await db.Orders.SingleAsync(order => order.Id == input.OrderId)).State);
        Assert.Equal(3, await db.Executions.CountAsync(receipt => receipt.OrderId == input.OrderId));
    }

    [Fact]
    public async Task DuplicateReservationsAndReleases_ArePersistedOncePerExecution()
    {
        var input = new OrderInput(Guid.NewGuid(), 1490m, PaymentFailure.None);
        var reserve = Context(DemoCommands.Reserve);
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Inventory(input, reserve)));
        var release = Context(DemoCommands.Release);
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Inventory(input, release)));

        await using var db = fixture.Inventory();
        Assert.Equal("Released", (await db.Orders.SingleAsync(order => order.Id == input.OrderId)).State);
        Assert.Equal(2, await db.Executions.CountAsync(receipt => receipt.OrderId == input.OrderId));
    }

    private async Task<OperationResult> Charge(OrderInput input, WorkflowCommandContext context)
    {
        await using var db = fixture.Payments();
        return await new PaymentHandler(db).HandleAsync(input, context, CancellationToken.None);
    }

    private async Task<OperationResult> Inventory(OrderInput input, WorkflowCommandContext context)
    {
        await using var db = fixture.Inventory();
        return await new InventoryHandler(db).HandleAsync(input, context, CancellationToken.None);
    }

    private static WorkflowCommandContext Context(string command) => new(
        new StepExecution(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), 0), command, Guid.NewGuid().ToString(),
        command is DemoCommands.Refund or DemoCommands.Release);
}
