namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StepInstanceTests
{
    [Fact]
    public void TryScheduleRetry_WhenRetriesRemain_SchedulesPendingAttemptAndClearsFailure()
    {
        // Arrange
        var step = new StepInstance(Guid.NewGuid(), Guid.NewGuid(), maxRetries: 2);
        step.Fail("failed");
        var delay = TimeSpan.FromSeconds(30);
        var before = DateTime.UtcNow;

        // Act
        var result = step.TryScheduleRetry(delay);
        var after = DateTime.UtcNow;

        // Assert
        Assert.True(result);
        Assert.Equal(1, step.RetryCount);
        Assert.Equal(StepStatus.Pending, step.Status);
        Assert.Null(step.Error);
        Assert.Null(step.CompletedAt);
        Assert.NotNull(step.NextRetryAt);
        Assert.InRange(step.NextRetryAt.GetValueOrDefault(), before + delay, after + delay);
    }

    [Fact]
    public void TryScheduleRetry_WhenRetryLimitReached_ReturnsFalseWithoutChangingState()
    {
        // Arrange
        var step = new StepInstance(Guid.NewGuid(), Guid.NewGuid(), 2, 2);
        step.Fail("final failure");
        var completedAt = step.CompletedAt;

        // Act
        var result = step.TryScheduleRetry(TimeSpan.FromSeconds(30));

        // Assert
        Assert.False(result);
        Assert.Equal(2, step.RetryCount);
        Assert.Equal(StepStatus.Failed, step.Status);
        Assert.Equal("final failure", step.Error);
        Assert.Equal(completedAt, step.CompletedAt);
        Assert.Null(step.NextRetryAt);
    }

    [Fact]
    public void TryScheduleRetry_WhenOneRetryRemains_AcceptsLastAllowedRetry()
    {
        // Arrange
        var step = new StepInstance(Guid.NewGuid(), Guid.NewGuid(), 1, 2);

        // Act
        var result = step.TryScheduleRetry(TimeSpan.Zero);

        // Assert
        Assert.True(result);
        Assert.Equal(2, step.RetryCount);
        Assert.Equal(StepStatus.Pending, step.Status);
    }

    [Fact]
    public void MarkRunning_WhenRetryScheduled_ClearsRetryDeadline()
    {
        // Arrange
        var step = new StepInstance(Guid.NewGuid(), Guid.NewGuid());
        step.TryScheduleRetry(TimeSpan.FromSeconds(30));

        // Act
        step.MarkRunning();

        // Assert
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Null(step.NextRetryAt);
    }
}