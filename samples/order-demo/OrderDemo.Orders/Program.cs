using System.Text.Json.Serialization;
using Grpc.Core;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.EntityFrameworkCore;
using WorkflowScheduler.Client;

namespace OrderDemo.Orders;

public partial class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Services.AddProblemDetails();
        builder.Services.ConfigureHttpJsonOptions(options =>
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

        builder.Services.AddDbContext<OrderDb>(options =>
            options.UseNpgsql(builder.Configuration.GetConnectionString("Database")));
        builder.Services.AddWorkflowSchedulerClient(options =>
            options.Address = new Uri(builder.Configuration["Scheduler:Address"]!));

        builder.Services.AddScoped<OrderWorkflow>();
        builder.Services.AddScoped<OrderDetailsReader>();

        foreach (var service in new[] { "inventory", "payments" })
            builder.Services.AddHttpClient(service, client =>
            {
                client.BaseAddress = new Uri(builder.Configuration[$"Services:{service}"]!);
                client.Timeout = TimeSpan.FromSeconds(3);
            });

        var app = builder.Build();

        await using (var scope = app.Services.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<OrderDb>().Database.EnsureCreatedAsync();

        app.UseExceptionHandler(new ExceptionHandlerOptions
        {
            AllowStatusCode404Response = true,
            ExceptionHandler = async context =>
            {
                var exception = context.Features.Get<IExceptionHandlerFeature>()?.Error;
                var status = exception switch
                {
                    WorkflowClientException { StatusCode: StatusCode.NotFound } => 404,
                    WorkflowClientException { StatusCode: StatusCode.FailedPrecondition } => 409,
                    WorkflowClientException { StatusCode: StatusCode.InvalidArgument } => 400,
                    WorkflowClientException or HttpRequestException => 503,
                    _ => 500
                };
                await Results.Problem(statusCode: status, detail: exception is WorkflowClientException workflow
                    ? workflow.Detail : "Не удалось выполнить запрос. Подробности — в журнале сервиса.")
                    .ExecuteAsync(context);
            }
        });

        app.MapGet("/health", async (OrderDb db, IWorkflowSchedulerClient scheduler,
            IHttpClientFactory httpClients, CancellationToken ct) =>
        {
            if (!await db.Database.CanConnectAsync(ct))
                return Results.StatusCode(503);

            foreach (var service in new[] { "inventory", "payments" })
            {
                using var response = await httpClients.CreateClient(service).GetAsync("health", ct);
                if (!response.IsSuccessStatusCode)
                    return Results.StatusCode(503);
            }

            try
            {
                await scheduler.GetDefinitionAsync(OrderWorkflow.Definition.Name, OrderWorkflow.Definition.Version, ct);
            }
            catch (WorkflowClientException exception) when (exception.StatusCode == StatusCode.NotFound)
            {
                return Results.Ok();
            }

            return Results.Ok();
        });

        app.MapOrders();
        await app.RunAsync();
    }
}
