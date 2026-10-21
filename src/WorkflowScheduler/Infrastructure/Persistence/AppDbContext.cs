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

    public async Task<ProcessInstance?> LoadActiveProcessAsync(Guid processId, CancellationToken ct)
    {
        var process = await ProcessInstances
            .Include(instance => instance.Steps.Where(step => step.Status == StepStatus.Pending
                                                              || step.Status == StepStatus.Running ||
                                                              step.Status == StepStatus.Waiting))
            .FirstOrDefaultAsync(instance => instance.Id == processId, ct);
        if (process is not null)
        {
            var lastSequence = await StepInstances
                .Where(step => step.ProcessInstanceId == processId)
                .MaxAsync(step => (int?)step.Sequence, ct);
            process.RestoreStepSequence(lastSequence ?? -1);
        }

        return process;
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(AppDbContext).Assembly);
    }
}