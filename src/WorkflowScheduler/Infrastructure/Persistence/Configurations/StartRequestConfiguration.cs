using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkflowScheduler.Domain.Processes;

namespace WorkflowScheduler.Infrastructure.Persistence.Configurations;

public class StartRequestConfiguration : IEntityTypeConfiguration<StartRequest>
{
    public void Configure(EntityTypeBuilder<StartRequest> builder)
    {
        builder.ToTable("start_requests");
        builder.HasKey(x => x.Id);
        builder.Property(x => x.RequestHash).HasMaxLength(64).IsRequired();
        builder.HasOne<ProcessInstance>().WithMany().HasForeignKey(x => x.ProcessInstanceId);
    }
}