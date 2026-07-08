namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxMessage
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public required string Type { get; init; }
    public required string Paxyload { get; init; }
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime? ProcessedAt { get; set; }
    public string? Error { get; set; }
}