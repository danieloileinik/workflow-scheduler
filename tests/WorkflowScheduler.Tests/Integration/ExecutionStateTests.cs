using WorkflowScheduler.Features.Steps.Queries.IsStepActive;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class ExecutionStateTests(TestInfrastructure infrastructure)
{
    [Theory]
    [InlineData("active", true)]
    [InlineData("cancelled", false)]
    [InlineData("completed", false)]
    [InlineData("old-attempt", false)]
    [InlineData("old-step", false)]
    [InlineData("wrong-definition", false)]
    [InlineData("wrong-compensation", false)]
    public async Task Query_ChecksPersistedExecutionWithoutLoadingHistory(string change, bool expected)
    {
        var app = await infrastructure.CreateHarnessAsync();
        var call = TestWorkflow.Call();
        var process = await app.Start(await app.Register(call));
        var snapshot = await app.Send(new GetProcessStatusQuery(process));
        var query = new IsStepActiveQuery(process, call.Id, snapshot!.CurrentStepId!.Value, 0, false);
        switch (change)
        {
            case "cancelled": await app.Send(new CancelProcessCommand(process)); break;
            case "completed": await app.Complete(process, call.Id); break;
            case "old-attempt": query = query with { Attempt = 1 }; break;
            case "old-step": query = query with { StepInstanceId = Guid.NewGuid() }; break;
            case "wrong-definition": query = query with { StepDefinitionId = Guid.NewGuid() }; break;
            case "wrong-compensation": query = query with { IsCompensation = true }; break;
        }

        Assert.Equal(expected, await app.Send(query));
    }
}