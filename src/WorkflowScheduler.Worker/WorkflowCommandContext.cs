using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WorkflowScheduler.Client;

namespace WorkflowScheduler.Worker;

public sealed record WorkflowCommandContext(
    StepExecution Execution,
    string Command,
    string CorrelationId,
    bool IsCompensation)
{
    public string IdempotencyKey =>
        $"{Execution.StepInstanceId:N}:{Execution.Attempt.ToString(CultureInfo.InvariantCulture)}";

    public string CallbackCorrelationId => $"worker:{IdempotencyKey}";
}

public sealed record WorkflowDeadLetter(
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    string? Payload,
    string Error)
{
    public bool PayloadTruncated { get; init; }
    public int? OriginalPayloadBytes { get; init; }
    public string? PayloadSha256 { get; init; }
    public bool KeyTruncated { get; init; }

    internal string Serialize(int maxBytes)
    {
        var json = JsonSerializer.Serialize(this);
        if (Encoding.UTF8.GetByteCount(json) <= maxBytes) return json;

        var reduced = this with
        {
            Key = Prefix(Key, 128),
            KeyTruncated = Key?.Length > 128,
            Payload = null,
            Error = Prefix(Error, 256)!,
            PayloadTruncated = Payload is not null,
            OriginalPayloadBytes = Payload is null ? null : Encoding.UTF8.GetByteCount(Payload),
            PayloadSha256 = Payload is null
                ? null
                : Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Payload)))
        };
        var overhead = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(reduced));
        if (overhead > maxBytes)
            throw new ArgumentException(
                "Dead-letter message limit cannot hold the diagnostic envelope.",
                nameof(maxBytes));

        // A UTF-16 code unit requires at most six bytes when escaped by the default JSON encoder.
        reduced = reduced with { Payload = Prefix(Payload, Math.Max(0, (maxBytes - overhead - 2) / 6)) };
        return JsonSerializer.Serialize(reduced);
    }

    private static string? Prefix(string? value, int length)
    {
        if (value is null || value.Length <= length) return value;
        if (length > 0 && char.IsHighSurrogate(value[length - 1])) length--;
        return value[..length];
    }
}