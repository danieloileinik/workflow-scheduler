using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class ProcessInstanceConfiguration : IEntityTypeConfiguration<ProcessInstance>
{
    public void Configure(EntityTypeBuilder<ProcessInstance> builder)
    {
        builder.ToTable("process_instances");
        builder.HasKey(i => i.Id);
        builder.Property(i => i.DefinitionId).IsRequired();
        builder.Property(i => i.BusinessKey).HasMaxLength(512).IsRequired();
        builder.Property(i => i.InputData).IsRequired();
        builder.Property(i => i.Status).HasConversion<string>().HasMaxLength(50).IsRequired();
        builder.Property(i => i.CurrentStepId);
        builder.Property(i => i.CreatedAt).IsRequired();
        builder.Property(i => i.UpdatedAt).IsRequired();
        builder.HasIndex(i => i.BusinessKey);
        builder.HasIndex(i => new { i.DefinitionId, i.Status });
        builder
            .HasMany(i => i.Steps)
            .WithOne()
            .HasForeignKey(s => s.ProcessInstanceId)
            .OnDelete(DeleteBehavior.Cascade);
        builder.Navigation(i => i.Steps).UsePropertyAccessMode(PropertyAccessMode.Field);
    }
}