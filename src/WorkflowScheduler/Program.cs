using WorkflowScheduler.Features.Processes.Commands.CancelProcess;
using WorkflowScheduler.Features.Processes.Commands.RegisterProcessDefinition;
using WorkflowScheduler.Features.Processes.Commands.SendEvent;
using WorkflowScheduler.Features.Processes.Commands.StartProcess;
using WorkflowScheduler.Features.Processes.Queries.GetProcessDefinition;
using WorkflowScheduler.Features.Processes.Queries.GetProcessStatus;
using WorkflowScheduler.Features.Steps.Commands.CompleteStep;
using WorkflowScheduler.Features.Steps.Queries.IsStepActive;
using WorkflowScheduler.Infrastructure.Persistence;

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

        app.MapGrpcService<RegisterProcessDefinitionEndpoint>();
        app.MapGrpcService<GetProcessDefinitionEndpoint>();
        app.MapGrpcService<StartProcessEndpoint>();
        app.MapGrpcService<CompleteStepEndpoint>();
        app.MapGrpcService<SendEventEndpoint>();
        app.MapGrpcService<CancelProcessEndpoint>();
        app.MapGrpcService<GetProcessStatusEndpoint>();
        app.MapGrpcService<IsStepActiveEndpoint>();
        await app.RunAsync();
    }
}