using WorkflowScheduler.Infrastructure.Persistence;
using ProcessService = WorkflowScheduler.Features.Processes.Endpoints.ProcessService;

namespace WorkflowScheduler;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddGrpc();
        builder.Services.AddWorkflowScheduler(builder.Configuration);
        var app = builder.Build();
        if (args.Contains("--initialize-database"))
        {
            await using var scope = app.Services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            await DatabaseInitializer.InitializeAsync(db);
            return;
        }

        app.MapGrpcService<ProcessService>();
        await app.RunAsync();
    }
}