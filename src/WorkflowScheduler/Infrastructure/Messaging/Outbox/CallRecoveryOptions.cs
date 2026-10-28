namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public sealed class CallRecoveryOptions
{
    public TimeSpan CallbackTimeout { get; init; } = TimeSpan.FromMinutes(5);
}
