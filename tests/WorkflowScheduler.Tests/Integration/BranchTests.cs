using System.Text.Json;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class BranchTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData("120.50", true)]
    [InlineData("20.50", false)]
    public async Task ConditionBranch_IsPersistedAndOnlySelectedCallIsPublished(string amount, bool expected)
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var yes = TestWorkflow.Call(service: "high");
        var no = TestWorkflow.Call(service: "low");
        var condition = new StepDefinitionDto(
            Guid.NewGuid(),
            "Amount check",
            StepType.Condition,
            JsonSerializer.Serialize(
                new
                {
                    inputKey = "amount", @operator = "gte", value = "100.50", trueNextStepId = yes.Id,
                    falseNextStepId = no.Id
                }),
            null,
            null);
        var definition = await app.Register(condition, yes, no);
        var start = new StartProcessCommand(definition, Guid.NewGuid(), "order", "{\"amount\":" + amount + "}");
        // Act
        var process = await app.Send(start);
        await app.Dispatch();
        var status = await app.Send(new GetProcessStatusQuery(process.Value.ProcessInstanceId));
        // Assert
        Assert.Equal("Completed", status!.StepHistory[0].Status);
        Assert.Equal(expected ? yes.Id : no.Id, status.StepHistory[1].StepDefinitionId);
        Assert.Single(app.Publisher.Messages);
        Assert.Equal(expected ? "process.commands.high" : "process.commands.low", app.Publisher.Messages[0].Topic);
    }

    [Fact]
    public async Task LoopThroughWait_CreatesNewExecutionAndRejectsEarlierExecutionCallback()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var wait = TestWorkflow.Wait(call.Id);
        call = call with { NextStepId = wait.Id };
        var process = await app.Start(await app.Register(call, wait));
        var initial = await app.Send(new GetProcessStatusQuery(process));
        var oldCallback = new CompleteStepCommand(
            process,
            call.Id,
            true,
            "{}",
            "first-result",
            initial!.StepHistory.Single().Id);
        // Act
        var first = await app.Send(oldCallback);
        var eventResult = await app.Send(new SendEventCommand(process, "Approved", "{}", "approval-1"));
        var stale = await app.Send(oldCallback with { CorrelationId = "late-first-result" });
        var repeated = await app.Send(new GetProcessStatusQuery(process));
        var second = await app.Complete(process, call.Id);
        // Assert
        Assert.False(first.IsError);
        Assert.False(eventResult.IsError);
        Assert.Equal("ProcessStep.StaleCallback", stale.FirstError.Code);
        Assert.Equal([call.Id, wait.Id, call.Id], repeated!.StepHistory.Select(x => x.StepDefinitionId));
        Assert.NotEqual(initial.StepHistory[0].Id, repeated.StepHistory[2].Id);
        Assert.Equal("Running", repeated.StepHistory[2].Status);
        Assert.False(second.IsError);
    }
}