namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ProcessInstanceTests
{
    [Fact]
    public void Start_WhenCreated_CreatesFirstStepAndSetsRunningStatus()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        var definitionId = Guid.NewGuid();

        // Act
        var result = process.Start(definitionId);

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(ProcessStatus.Running, process.Status);
        Assert.Equal(result.Value.Id, process.CurrentStepId);
        Assert.Equal(definitionId, result.Value.StepDefinitionId);
        Assert.Equal(process.Id, result.Value.ProcessInstanceId);
        Assert.Equal(0, result.Value.Sequence);
        Assert.Single(process.Steps);
        Assert.True(process.UpdatedAt >= process.CreatedAt);
    }

    [Fact]
    public void Start_WhenCompleted_ReturnsInvalidTransitionWithoutAddingStep()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());
        process.Complete();

        // Act
        var result = process.Start(Guid.NewGuid());

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Completed, process.Status);
        Assert.Single(process.Steps);
    }

    [Fact]
    public void MoveToStep_WhenRunning_AppendsStepAndUpdatesCurrentStep()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());
        var nextDefinitionId = Guid.NewGuid();

        // Act
        var result = process.MoveToStep(nextDefinitionId);

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(nextDefinitionId, result.Value.StepDefinitionId);
        Assert.Equal(result.Value.Id, process.CurrentStepId);
        Assert.Equal([0, 1], process.Steps.Select(step => step.Sequence));
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public void MoveToStep_WhenWaiting_ReturnsInvalidTransitionWithoutAddingStep()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        var first = process.Start(Guid.NewGuid());
        process.Wait();

        // Act
        var result = process.MoveToStep(Guid.NewGuid());

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Waiting, process.Status);
        Assert.Equal(first.Value.Id, process.CurrentStepId);
        Assert.Single(process.Steps);
    }

    [Fact]
    public void Wait_WhenRunning_SetsWaitingStatus()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());

        // Act
        var result = process.Wait();

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(ProcessStatus.Waiting, process.Status);
    }

    [Fact]
    public void Wait_WhenCreated_ReturnsInvalidTransition()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");

        // Act
        var result = process.Wait();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Created, process.Status);
    }

    [Fact]
    public void Resume_WhenWaiting_SetsRunningStatus()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());
        process.Wait();

        // Act
        var result = process.Resume();

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(ProcessStatus.Running, process.Status);
    }

    [Fact]
    public void Resume_WhenCreated_ReturnsInvalidTransition()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");

        // Act
        var result = process.Resume();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Created, process.Status);
    }

    [Fact]
    public void Complete_WhenRunning_SetsCompletedStatus()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());

        // Act
        var result = process.Complete();

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(ProcessStatus.Completed, process.Status);
    }

    [Fact]
    public void Complete_WhenCreated_ReturnsInvalidTransition()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");

        // Act
        var result = process.Complete();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Created, process.Status);
    }

    [Fact]
    public void Cancel_WhenCompleted_ReturnsAlreadyCompletedError()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());
        process.Complete();

        // Act
        var result = process.Cancel();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.AlreadyCompleted", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Completed, process.Status);
    }

    [Fact]
    public void Fail_WhenCompleted_ReturnsInvalidTransition()
    {
        // Arrange
        var process = new ProcessInstance(Guid.NewGuid(), "order", "{}");
        process.Start(Guid.NewGuid());
        process.Complete();

        // Act
        var result = process.Fail();

        // Assert
        Assert.True(result.IsError);
        Assert.Equal("Process.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ProcessStatus.Completed, process.Status);
    }
}