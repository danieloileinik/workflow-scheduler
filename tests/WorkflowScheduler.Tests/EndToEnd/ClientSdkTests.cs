using Grpc.Core;
using WorkflowScheduler.Client;
using WorkflowScheduler.Contracts.Events;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.EndToEnd;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "EndToEnd")]
public class ClientSdkTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData(150, "high")]
    [InlineData(20, "low")]
    public async Task FluentDefinition_RunsTypedPayloadThroughBranchesEventsAndKafka(int amount, string selected)
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        using var broker = infrastructure.CreateProbe("billing", "high", "low", "shipping");
        var definition = Workflow
            .Define("sdk-order-" + Guid.NewGuid())
            .Call("billing", "Charge")
            .CompensateWith("refund", "Refund")
            .WaitFor("Approved")
            .Condition(
                "amount",
                WorkflowComparison.GreaterThanOrEqual,
                100m,
                yes => yes.Call("high", "High"),
                no => no.Call("low", "Low"))
            .Call("shipping", "Ship")
            .Build();
        var registered = await client.RegisterAsync(definition);
        var input = new { OrderId = 42, Amount = amount };
        var key = Guid.NewGuid();

        var process = await client.StartAsync(registered, input, "order-42", key);
        var replay = await client.StartAsync(registered, input, "order-42", key);
        var billing = await broker.ReadAsync("billing", process);
        await client.CompleteStepAsync(Execution(billing), new { Charged = true }, "billing-callback");
        var waiting = await client.GetStatusAsync(process);
        await client.SendEventAsync(process, "Approved", new { Actor = "manager" }, "approval-42");
        var branch = await broker.ReadAsync(selected, process);
        await client.CompleteStepAsync(Execution(branch), new { Accepted = true }, "branch-callback");
        var shipping = await broker.ReadAsync("shipping", process);
        await client.CompleteStepAsync(Execution(shipping), new { Shipped = true }, "shipping-callback");
        var completed = await client.GetStatusAsync(process);
        await app.WaitForOutboxAsync(3);

        Assert.Equal(process, replay);
        Assert.Equal(WorkflowProcessStatus.Waiting, waiting.Status);
        Assert.Equal(WorkflowStepStatus.Waiting, waiting.StepHistory.Last().Status);
        Assert.Equal(WorkflowProcessStatus.Completed, completed.Status);
        Assert.Equal(5, completed.StepHistory.Count);
        Assert.All(completed.StepHistory, step => Assert.Equal(WorkflowStepStatus.Completed, step.Status));
        Assert.True(completed.StepHistory[0].ReadOutput<Dictionary<string, bool>>()!["charged"]);
        Assert.Equal(billing.InputData, shipping.InputData);
        Assert.Equal(0, broker.CountRemaining(process));
        Assert.Equal(1, await app.WithDb(db => db.ProcessInstances.CountAsync()));
    }

    [Fact]
    public async Task Client_ReportsConflictsMissingProcessesFailureRetriesAndCancellation()
    {
        await using var app = await infrastructure.CreateGrpcAppAsync();
        app.StartServer();
        using var client =
            new WorkflowSchedulerClient(new WorkflowClientOptions { Address = app.ClientOptions.BaseAddress });
        using var broker = infrastructure.CreateProbe("billing");
        var definition = Workflow.Define("sdk-cancel-" + Guid.NewGuid()).Call("billing", "Charge").Build();
        var registered = await client.RegisterAsync(definition);
        var registrationConflict =
            await Assert.ThrowsAsync<WorkflowClientException>(() => client.RegisterAsync(definition));
        var missing = await Assert.ThrowsAsync<WorkflowClientException>(() => client.GetStatusAsync(Guid.NewGuid()));
        var process = await client.StartAsync(registered, new { OrderId = 42 }, "order", Guid.NewGuid());
        var command = await broker.ReadAsync("billing", process);
        await client.FailStepAsync(Execution(command), "temporary failure", "failed-call");
        var pending = await client.GetStatusAsync(process);
        await client.CancelAsync(process);
        var cancelled = await client.GetStatusAsync(process);
        var late = await Assert.ThrowsAsync<WorkflowClientException>(() =>
            client.CompleteStepAsync(Execution(command), new { }, "late-callback"));

        Assert.Equal(StatusCode.FailedPrecondition, registrationConflict.StatusCode);
        Assert.Equal(StatusCode.NotFound, missing.StatusCode);
        Assert.Equal(WorkflowStepStatus.Pending, pending.StepHistory.Single().Status);
        Assert.Equal(1, pending.StepHistory.Single().RetryCount);
        Assert.Equal(WorkflowProcessStatus.Cancelled, cancelled.Status);
        Assert.Equal(StatusCode.FailedPrecondition, late.StatusCode);
    }

    private static StepExecution Execution(StepCommandEvent command)
    {
        return new StepExecution(
            command.ProcessInstanceId,
            command.StepDefinitionId,
            command.StepInstanceId,
            command.Attempt);
    }
}