namespace WorkflowScheduler.Domain.Processes;

public class EventLog(Guid id, Guid processInstanceId, string eventName, string correlationId)
{
    public EventLog(Guid processInstanceId, string eventName, string correlationId)
        : this(Guid.NewGuid(), processInstanceId, eventName, correlationId)
    {
    }

    public Guid Id { get; init; } = id;
    public Guid ProcessInstanceId { get; init; } = processInstanceId;
    public string EventName { get; init; } = eventName;
    public string CorrelationId { get; init; } = correlationId;
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
}