using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable("outbox_messages");
        builder.HasKey(m => m.Id);
        builder.Property(m => m.Type).HasMaxLength(256).IsRequired();
        builder.Property(m => m.Payload).IsRequired();
        builder.Property(m => m.CreatedAt).IsRequired();
        builder.Property(m => m.ProcessedAt);
        builder.Property(m => m.Error).HasMaxLength(2048);
        builder.Property(m => m.Attempts).HasDefaultValue(0);
        builder.HasIndex(m => m.ClaimId).HasFilter("\"ClaimId\" IS NOT NULL");
        builder.HasIndex(m => new { m.NextAttemptAt, m.CreatedAt, m.Id })
            .HasFilter("\"ProcessedAt\" IS NULL AND \"FailedAt\" IS NULL");
    }
}
