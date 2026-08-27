using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class EventLogConfiguration : IEntityTypeConfiguration<EventLog>
{
    public void Configure(EntityTypeBuilder<EventLog> builder)
    {
        builder.ToTable("event_logs");
        builder.HasKey(e => e.Id);
        builder.Property(e => e.ProcessInstanceId).IsRequired();
        builder.Property(e => e.EventName).HasMaxLength(256).IsRequired();
        builder.Property(e => e.CorrelationId).HasMaxLength(256).IsRequired();
        builder.Property(e => e.CreatedAt).IsRequired();
        builder.HasIndex(e => new { e.ProcessInstanceId, e.EventName, e.CorrelationId }).IsUnique();
    }
}