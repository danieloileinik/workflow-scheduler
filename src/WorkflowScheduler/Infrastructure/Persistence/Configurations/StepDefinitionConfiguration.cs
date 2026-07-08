using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class StepDefinitionConfiguration : IEntityTypeConfiguration<StepDefinition>
{
    public void Configure(EntityTypeBuilder<StepDefinition> builder)
    {
        builder.ToTable("step_definitions");
        builder.HasKey(s => s.Id);
        builder.Property(s => s.ProcessDefinitionId).IsRequired();
        builder.Property(s => s.Name).HasMaxLength(256).IsRequired();
        builder.Property(s => s.Type).HasConversion<string>().HasMaxLength(50).IsRequired();
        builder.Property(s => s.Metadata).IsRequired();
        builder.Property(s => s.NextStepId);
        builder.Property(s => s.CompensationStepId);
    }
}