using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class SagaCompensationLogConfiguration : IEntityTypeConfiguration<SagaCompensationLog>
{
    public void Configure(EntityTypeBuilder<SagaCompensationLog> builder)
    {
        builder.ToTable("saga_compensation_logs");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.Status).HasConversion<string>();
        builder.Property(x => x.Error).HasMaxLength(2048);
        builder.HasIndex(x => new { x.ProcessInstanceId, x.OriginalStepInstanceId }).IsUnique();
    }
}