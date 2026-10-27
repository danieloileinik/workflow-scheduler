using Confluent.Kafka;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using WorkflowScheduler.Client;

namespace WorkflowScheduler.Worker;

internal sealed class WorkerRetry(WorkflowWorkerOptions options, ILogger logger)
{
    public async Task<T> RunAsync<T>(string operation, Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(options.RpcTimeout);

            try
            {
                return await action(timeout.Token);
            }
            catch (OperationCanceledException exception) when (!ct.IsCancellationRequested)
            {
                if (attempt == options.MaxDeliveryAttempts)
                    throw new TimeoutException($"{operation} timed out after {attempt} attempts.", exception);

                logger.LogWarning("{Operation} timed out; retry {Attempt}", operation, attempt);
            }
            catch (Exception exception) when (attempt < options.MaxDeliveryAttempts && IsTransient(exception))
            {
                logger.LogWarning(exception, "{Operation} failed; retry {Attempt}", operation, attempt);
            }

            await Task.Delay(options.RetryDelay, ct);
        }
    }

    private static bool IsTransient(Exception exception)
    {
        return exception switch
        {
            WorkflowClientException client => client.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded
                or StatusCode.ResourceExhausted or StatusCode.Aborted or StatusCode.FailedPrecondition,
            KafkaException kafka => !kafka.Error.IsFatal,
            _ => false
        };
    }
}