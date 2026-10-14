using System.Net;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using WorkflowScheduler.Grpc;

namespace WorkflowScheduler.Tests.Support;

internal sealed class GrpcApp : WebApplicationFactory<Program>
{
    private readonly TestInfrastructure _infrastructure;
    private GrpcChannel? _channel;
    private Processes.ProcessesClient? _client;
    private HttpClient? _httpClient;

    public GrpcApp(TestInfrastructure infrastructure)
    {
        _infrastructure = infrastructure;
        UseKestrel(options => options.Listen(
            IPAddress.Loopback,
            0,
            endpoint => endpoint.Protocols = HttpProtocols.Http2));
    }

    public Processes.ProcessesClient Client
    {
        get
        {
            // Preserve the address assigned to Kestrel's ephemeral port. The overload
            // taking client options overrides it with the options' default localhost URL.
            if (_httpClient is null)
            {
                _httpClient = CreateDefaultClient();
                _httpClient.Timeout = Timeout.InfiniteTimeSpan;
            }

            _channel ??= GrpcChannel.ForAddress(
                _httpClient.BaseAddress!,
                new GrpcChannelOptions { HttpClient = _httpClient });
            return _client ??= new Processes.ProcessesClient(_channel.Intercept(new RpcDeadlineInterceptor()));
        }
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, configuration) => configuration.AddConfiguration(
            _infrastructure.Configuration(_infrastructure.EndToEndPostgres, 1)));
    }

    public async Task<T> WithDb<T>(Func<AppDbContext, Task<T>> action)
    {
        await using var scope = Services.CreateAsyncScope();
        return await action(scope.ServiceProvider.GetRequiredService<AppDbContext>());
    }

    public async Task WaitForOutboxAsync(int expected)
    {
        await Eventually.UntilAsync(
            ct => WithDb(db => db.OutboxMessages.CountAsync(message => message.ProcessedAt != null, ct)),
            count => count == expected,
            $"Expected {expected} processed outbox messages");
    }

    public override async ValueTask DisposeAsync()
    {
        _channel?.Dispose();
        _httpClient?.Dispose();
        await base.DisposeAsync();
    }

    public static RegisterProcessDefinitionRequest Registration(params StepDefinitionDto[] steps)
    {
        var request = new RegisterProcessDefinitionRequest { Name = "workflow-" + Guid.NewGuid(), Version = 1 };
        foreach (var step in steps)
            request.Steps.Add(
                new StepDefinitionMessage
                {
                    Id = step.Id.ToString(), Name = step.Name, Metadata = step.Metadata,
                    StepType = step.Type switch
                    {
                        StepType.Call => StepTypeMessage.Call,
                        StepType.WaitEvent => StepTypeMessage.WaitEvent,
                        _ => StepTypeMessage.Condition
                    },
                    NextStepId = step.NextStepId?.ToString() ?? "",
                    CompensationStepId = step.CompensationStepId?.ToString() ?? ""
                });
        return request;
    }

    private sealed class RpcDeadlineInterceptor : Interceptor
    {
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            var options = context.Options.WithDeadline(context.Options.Deadline ?? DateTime.UtcNow.AddSeconds(20));
            return continuation(
                request,
                new ClientInterceptorContext<TRequest, TResponse>(
                    context.Method,
                    context.Host,
                    options));
        }
    }
}