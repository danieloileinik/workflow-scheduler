using System.Text.Json;
using OrderDemo.Contracts;
using WorkflowScheduler.Client;

namespace OrderDemo.Orders;

public sealed class OrderDetailsReader(
    IWorkflowSchedulerClient scheduler,
    IHttpClientFactory httpClients,
    ILogger<OrderDetailsReader> logger)
{
    public async Task<OrderDetails> ReadAsync(DemoOrder order, CancellationToken ct)
    {
        var inventory = ReadServiceAsync("inventory", order.Id, ct);
        var payment = ReadServiceAsync("payments", order.Id, ct);
        var processTask = ReadProcessAsync(order.ProcessId, ct);
        await Task.WhenAll(inventory, payment, processTask);
        var process = await processTask;
        var snapshots = await Task.WhenAll(inventory, payment);
        return new OrderDetails(order, process, snapshots[0].Snapshot, snapshots[1].Snapshot,
            snapshots.Where(value => value.Warning is not null).Select(value => value.Warning!).ToArray());
    }

    private async Task<WorkflowProcessSnapshot?> ReadProcessAsync(Guid? id, CancellationToken ct) =>
        id is Guid processId
            ? await scheduler.GetStatusPageAsync(processId, pageSize: 100, includeOutputs: true, cancellationToken: ct)
            : null;

    private async Task<(ServiceSnapshot? Snapshot, string? Warning)> ReadServiceAsync(string service, Guid id,
        CancellationToken ct)
    {
        try
        {
            var snapshot = await httpClients.CreateClient(service).GetFromJsonAsync<ServiceSnapshot>($"orders/{id}", ct);
            return snapshot is null ? (null, $"{service}: empty response") : (snapshot, null);
        }
        catch (Exception exception) when (exception is HttpRequestException or JsonException
                                         || exception is OperationCanceledException && !ct.IsCancellationRequested)
        {
            logger.LogWarning(exception, "Cannot read {Service} state for order {OrderId}", service, id);
            return (null, $"{service}: сервис временно недоступен");
        }
    }
}
