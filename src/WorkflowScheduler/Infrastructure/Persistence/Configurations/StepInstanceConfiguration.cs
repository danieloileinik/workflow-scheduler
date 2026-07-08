using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class StepInstanceConfiguration : IEntityTypeConfiguration<StepInstance>
{
    public void Configure(EntityTypeBuilder<StepInstance> builder)
    {
        builder.ToTable("step_instances");
        builder.HasKey(s => s.Id);
        builder.Property(s => s.ProcessInstanceId).IsRequired();
        builder.Property(s => s.StepDefinitionId).IsRequired();
        builder.Property(s => s.Status).HasConversion<string>().HasMaxLength(50).IsRequired();
        builder.Property(s => s.MaxRetries).IsRequired();
        builder.Property(s => s.RetryCount).IsRequired();
        builder.Property(s => s.StartedAt).IsRequired();
        builder.Property(s => s.CompletedAt);
        builder.Property(s => s.NextRetryAt);
        builder.Property(s => s.TimeoutAt);
        builder.Property(s => s.OutputData);
        builder.Property(s => s.Error).HasMaxLength(2048);
        builder.HasIndex(s => new { s.ProcessInstanceId, s.StepDefinitionId });
        builder.HasIndex(s => s.NextRetryAt).HasFilter("next_retry_at IS NOT NULL");
    }
}