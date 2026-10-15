namespace WorkflowScheduler.Infrastructure.Messaging.Outbox;

public class OutboxMessage
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public required string Type { get; init; }
    public required string Payload { get; init; }
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime? ProcessedAt { get; set; }
    public string? Error { get; set; }
    public int Attempts { get; set; }
    public DateTime? NextAttemptAt { get; set; }
    public DateTime? FailedAt { get; set; }
    public Guid? ClaimId { get; set; }
    public DateTime? ClaimedUntil { get; set; }
}
