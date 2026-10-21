using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Features.Steps.Commands.RetrySteps;
using WorkflowScheduler.Tests.Support;

namespace WorkflowScheduler.Tests.Integration;

[Collection(InfrastructureCollection.Name)]
[Trait("Category", "Integration")]
public class BoundedExecutionHistoryTests(TestInfrastructure infrastructure)
{
    [Fact]
    public async Task LoopRetryAndCompensation_LoadOnlyActiveExecutionsAndKeepGlobalSequences()
    {
        // Arrange
        var app = await infrastructure.CreateHarnessAsync();
        var undo = TestWorkflow.Call();
        var wait = TestWorkflow.Wait();
        var call = TestWorkflow.Call(wait.Id, undo.Id);
        wait = wait with { NextStepId = call.Id };
        var processId = await app.Start(await app.Register(call, wait, undo));
        var currentId = await app.WithDb(db => db
            .ProcessInstances
            .Where(process => process.Id == processId)
            .Select(process => process.CurrentStepId!.Value)
            .SingleAsync());
        await app.WithDb(async db =>
        {
            for (var sequence = 1; sequence <= 12; sequence++)
            {
                var historical = new StepInstance(processId, call.Id);
                historical.SetSequence(sequence);
                historical.MarkRunning();
                historical.Complete(new string('x', 250_000));
                db.StepInstances.Add(historical);
            }

            return await db.SaveChangesAsync();
        });
        var probe = new StepMaterializationProbe();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<AppDbContext>(options => options.AddInterceptors(probe));
        services.AddWorkflowScheduler(infrastructure.Configuration(infrastructure.IntegrationPostgres, 0));
        await using var provider = services.BuildServiceProvider();

        async Task Send(IRequest<ErrorOr<Success>> command)
        {
            probe.Steps.Clear();
            await using var scope = provider.CreateAsyncScope();
            var result = await scope.ServiceProvider.GetRequiredService<IMediator>().Send(command);
            Assert.False(result.IsError, result.IsError ? result.FirstError.Description : "");
            Assert.Single(probe.Steps);
            Assert.All(probe.Steps, step => Assert.True(step.OutputData is null || step.OutputData.Length < 100));
        }

        // Act
        await Send(new CompleteStepCommand(processId, call.Id, true, "{}", "forward", currentId));
        await Send(new SendEventCommand(processId, "Approved", "{}", "approved"));
        currentId = await app.WithDb(db => db
            .ProcessInstances
            .Where(process => process.Id == processId)
            .Select(process => process.CurrentStepId!.Value)
            .SingleAsync());
        await Send(new CompleteStepCommand(processId, call.Id, false, "failure", "failed", currentId));
        await app.WithDb(db =>
            db
                .StepInstances
                .Where(step => step.Id == currentId)
                .ExecuteUpdateAsync(update => update.SetProperty(
                    step => step.NextRetryAt,
                    DateTime.UtcNow.AddSeconds(-1))));
        await Send(new RetryStepsCommand(processId));
        await Send(new CancelProcessCommand(processId));
        for (var index = 0; index < 13; index++)
        {
            var execution = await app.WithDb(db => db
                .StepInstances
                .Where(step => step.ProcessInstanceId == processId && step.IsCompensation
                                                                   && step.Status == StepStatus.Running)
                .Select(step => new { step.Id, step.StepDefinitionId })
                .SingleAsync());
            await Send(
                new CompleteStepCommand(
                    processId,
                    execution.StepDefinitionId,
                    true,
                    "{}",
                    "undo-" + index,
                    execution.Id));
        }

        // Assert
        var steps = await app.WithDb(db => db
            .StepInstances
            .Where(step => step.ProcessInstanceId == processId)
            .OrderBy(step => step.Sequence)
            .Select(step => new { step.Sequence, step.Status, step.IsCompensation })
            .ToListAsync());
        Assert.Equal(Enumerable.Range(0, 28), steps.Select(step => step.Sequence));
        Assert.Equal(13, steps.Count(step => step.Status == StepStatus.Compensated));
        Assert.Equal(13, steps.Count(step => step.IsCompensation && step.Status == StepStatus.Completed));
        Assert.Equal(StepStatus.Cancelled, steps[14].Status);
    }
}