namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class StepInstanceTests
{
    [Theory]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Failed)]
    [InlineData(StepStatus.TimedOut)]
    public void TryScheduleRetry_WhenRetriesRemain_SchedulesPendingAttemptAndClearsFailure(StepStatus status)
    {
        // Arrange
        var step = CreateStep(status, maxRetries: 2);
        var delay = TimeSpan.FromSeconds(30);
        var before = DateTime.UtcNow;

        // Act
        var result = step.TryScheduleRetry(delay);
        var after = DateTime.UtcNow;

        // Assert
        Assert.False(result.IsError);
        Assert.True(result.Value);
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
        var step = CreateStep(StepStatus.Failed, 2, 2);
        var completedAt = step.CompletedAt;

        // Act
        var result = step.TryScheduleRetry(TimeSpan.FromSeconds(30));

        // Assert
        Assert.False(result.IsError);
        Assert.False(result.Value);
        Assert.Equal(2, step.RetryCount);
        Assert.Equal(StepStatus.Failed, step.Status);
        Assert.Equal("saved failure", step.Error);
        Assert.Equal(completedAt, step.CompletedAt);
        Assert.Null(step.NextRetryAt);
    }

    [Fact]
    public void TryScheduleRetry_WhenOneRetryRemains_AcceptsLastAllowedRetry()
    {
        // Arrange
        var step = CreateStep(StepStatus.Running, 1, 2);

        // Act
        var result = step.TryScheduleRetry(TimeSpan.Zero);

        // Assert
        Assert.False(result.IsError);
        Assert.True(result.Value);
        Assert.Equal(2, step.RetryCount);
        Assert.Equal(StepStatus.Pending, step.Status);
    }

    [Fact]
    public void MarkRunning_WhenRetryScheduled_ClearsRetryDeadline()
    {
        // Arrange
        var step = CreateStep(StepStatus.Running);
        Assert.False(step.TryScheduleRetry(TimeSpan.FromSeconds(30)).IsError);

        // Act
        var result = step.MarkRunning();

        // Assert
        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Running, step.Status);
        Assert.Null(step.NextRetryAt);
    }

    [Theory]
    [InlineData(StepStatus.Completed)]
    [InlineData(StepStatus.Cancelled)]
    [InlineData(StepStatus.Compensated)]
    public void TerminalStep_CannotRestartCompleteFailWaitCancelTimeOutOrRetry(StepStatus status)
    {
        var step = CreateStep(status);

        AssertRejected(step, current => current.MarkRunning());
        AssertRejected(step, current => current.MarkWaiting());
        AssertRejected(step, current => current.Complete("replacement output"));
        AssertRejected(step, current => current.Fail("replacement failure"));
        AssertRejected(step, current => current.Cancel());
        AssertRejected(step, current => current.MarkTimedOut());
        AssertRejected(step, current => current.TryScheduleRetry(TimeSpan.Zero));
    }

    [Fact]
    public void PendingStep_CannotWaitCompleteFailTimeOutOrRetryBeforeStarting()
    {
        var step = CreateStep(StepStatus.Pending);

        AssertRejected(step, current => current.MarkWaiting());
        AssertRejected(step, current => current.Complete("output"));
        AssertRejected(step, current => current.Fail("failure"));
        AssertRejected(step, current => current.MarkTimedOut());
        AssertRejected(step, current => current.TryScheduleRetry(TimeSpan.Zero));
    }

    [Theory]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    public void ActiveStep_CannotStartAgain(StepStatus status)
    {
        var step = CreateStep(status);

        AssertRejected(step, current => current.MarkRunning());
    }

    [Theory]
    [InlineData(StepStatus.Failed)]
    [InlineData(StepStatus.TimedOut)]
    public void FailedExecution_CanOnlyResumeThroughScheduledRetry(StepStatus status)
    {
        var step = CreateStep(status);

        AssertRejected(step, current => current.MarkRunning());
        AssertRejected(step, current => current.MarkWaiting());
        AssertRejected(step, current => current.Complete("output"));
        AssertRejected(step, current => current.Fail("replacement failure"));
        AssertRejected(step, current => current.Cancel());
        AssertRejected(step, current => current.MarkTimedOut());
    }

    [Fact]
    public void WaitingStep_CannotWaitAgainOrScheduleRetryWithoutFailure()
    {
        var step = CreateStep(StepStatus.Waiting);

        AssertRejected(step, current => current.MarkWaiting());
        AssertRejected(step, current => current.TryScheduleRetry(TimeSpan.Zero));
    }

    [Fact]
    public void ScheduledRetry_CannotBeScheduledAgainOrCompletedBeforeDispatch()
    {
        var step = CreateStep(StepStatus.Running);
        Assert.False(step.TryScheduleRetry(TimeSpan.FromSeconds(30)).IsError);

        AssertRejected(step, current => current.TryScheduleRetry(TimeSpan.Zero));
        AssertRejected(step, current => current.Complete("late output"));
    }

    [Theory]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    public void Complete_WhenActive_RecordsOutputAndCompletionTime(StepStatus status)
    {
        var step = CreateStep(status);
        var before = DateTime.UtcNow;

        var result = step.Complete("output");
        var after = DateTime.UtcNow;

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Completed, step.Status);
        Assert.Equal("output", step.OutputData);
        Assert.InRange(step.CompletedAt!.Value, before, after);
    }

    [Fact]
    public void MarkWaiting_WhenRunning_WaitsWithoutCompletingExecution()
    {
        var step = CreateStep(StepStatus.Running);

        var result = step.MarkWaiting();

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Waiting, step.Status);
        Assert.Null(step.CompletedAt);
    }

    [Theory]
    [InlineData(StepStatus.Pending)]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    public void Cancel_WhenActiveOrPending_StopsExecution(StepStatus status)
    {
        var step = CreateStep(status);

        var result = step.Cancel();

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Cancelled, step.Status);
        Assert.NotNull(step.CompletedAt);
        Assert.Null(step.NextRetryAt);
    }

    [Fact]
    public void Cancel_WhenRetryScheduled_ClearsRetryDeadline()
    {
        var step = CreateStep(StepStatus.Running);
        Assert.False(step.TryScheduleRetry(TimeSpan.FromSeconds(30)).IsError);

        var result = step.Cancel();

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Cancelled, step.Status);
        Assert.Null(step.NextRetryAt);
    }

    [Theory]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    public void Fail_WhenActive_RecordsFailure(StepStatus status)
    {
        var step = CreateStep(status);

        var result = step.Fail("failure");

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Failed, step.Status);
        Assert.Equal("failure", step.Error);
        Assert.NotNull(step.CompletedAt);
    }

    [Theory]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    public void MarkTimedOut_WhenActive_RecordsTimeout(StepStatus status)
    {
        var step = CreateStep(status);

        var result = step.MarkTimedOut();

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.TimedOut, step.Status);
        Assert.Equal("Step execution timed out", step.Error);
        Assert.NotNull(step.CompletedAt);
    }

    [Theory]
    [InlineData(StepStatus.Pending)]
    [InlineData(StepStatus.Running)]
    [InlineData(StepStatus.Waiting)]
    [InlineData(StepStatus.Failed)]
    [InlineData(StepStatus.TimedOut)]
    [InlineData(StepStatus.Cancelled)]
    [InlineData(StepStatus.Compensated)]
    public void MarkCompensated_WhenNotCompleted_ReturnsErrorWithoutChangingState(StepStatus status)
    {
        var step = CreateStep(status);

        AssertRejected(step, current => current.MarkCompensated());
    }

    [Fact]
    public void MarkCompensated_WhenCompleted_PreservesOriginalResult()
    {
        var step = CreateStep(StepStatus.Completed);
        var completedAt = step.CompletedAt;

        var result = step.MarkCompensated();

        Assert.False(result.IsError);
        Assert.Equal(StepStatus.Compensated, step.Status);
        Assert.Equal("saved output", step.OutputData);
        Assert.Equal(completedAt, step.CompletedAt);
    }

    private static void AssertRejected<T>(StepInstance step, Func<StepInstance, ErrorOr<T>> transition)
    {
        var before = (step.Status, step.RetryCount, step.NextRetryAt, step.CompletedAt, step.OutputData, step.Error);

        var result = transition(step);

        Assert.True(result.IsError);
        Assert.Equal("ProcessStep.InvalidTransition", result.FirstError.Code);
        Assert.Equal(ErrorType.Conflict, result.FirstError.Type);
        Assert.Equal(
            before,
            (step.Status, step.RetryCount, step.NextRetryAt, step.CompletedAt, step.OutputData, step.Error));
    }

    private static StepInstance CreateStep(StepStatus status, int retryCount = 0, int maxRetries = 3)
    {
        var step = new StepInstance(Guid.NewGuid(), Guid.NewGuid(), retryCount, maxRetries);
        if (status == StepStatus.Pending) return step;
        Assert.False(step.MarkRunning().IsError);
        switch (status)
        {
            case StepStatus.Waiting:
                Assert.False(step.MarkWaiting().IsError);
                break;
            case StepStatus.Completed:
                Assert.False(step.Complete("saved output").IsError);
                break;
            case StepStatus.Failed:
                Assert.False(step.Fail("saved failure").IsError);
                break;
            case StepStatus.TimedOut:
                Assert.False(step.MarkTimedOut().IsError);
                break;
            case StepStatus.Cancelled:
                Assert.False(step.Cancel().IsError);
                break;
            case StepStatus.Compensated:
                Assert.False(step.Complete("saved output").IsError);
                Assert.False(step.MarkCompensated().IsError);
                break;
        }

        Assert.Equal(status, step.Status);
        return step;
    }
}