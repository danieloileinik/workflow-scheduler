using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;
using WorkflowScheduler.Infrastructure.Messaging.Outbox;

namespace WorkflowScheduler.Infrastructure.Persistence;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<StartRequest> StartRequests => Set<StartRequest>();
    public DbSet<ProcessDefinition> ProcessDefinitions => Set<ProcessDefinition>();
    public DbSet<ProcessInstance> ProcessInstances => Set<ProcessInstance>();
    public DbSet<StepDefinition> StepDefinitions => Set<StepDefinition>();
    public DbSet<StepInstance> StepInstances => Set<StepInstance>();
    public DbSet<EventLog> EventLogs => Set<EventLog>();
    public DbSet<SagaCompensationLog> SagaCompensationLogs => Set<SagaCompensationLog>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
    }
}