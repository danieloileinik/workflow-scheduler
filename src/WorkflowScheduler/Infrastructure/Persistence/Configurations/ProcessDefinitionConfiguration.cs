using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class ProcessDefinitionConfiguration : IEntityTypeConfiguration<ProcessDefinition>
{
    public void Configure(EntityTypeBuilder<ProcessDefinition> builder)
    {
        builder.ToTable("process_definitions");
        builder.HasKey(d => d.Id);
        builder.Property(d => d.Name).HasMaxLength(256).IsRequired();
        builder.Property(d => d.Version).IsRequired();
        builder.Property(d => d.CreatedAt).IsRequired();
        builder.HasIndex(d => new { d.Name, d.Version }).IsUnique();
        builder
            .HasMany(d => d.Steps)
            .WithOne()
            .HasForeignKey(s => s.ProcessDefinitionId)
            .OnDelete(DeleteBehavior.Cascade);
        builder.Navigation(d => d.Steps).UsePropertyAccessMode(PropertyAccessMode.Field);
    }
}