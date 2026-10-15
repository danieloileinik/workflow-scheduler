using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;

namespace WorkflowScheduler.Client;

public sealed class WorkflowClientOptions
{
    public Uri? Address { get; set; }
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);
    public GrpcChannelOptions? ChannelOptions { get; set; }

    /// <summary>Called for each RPC; can supply authorization metadata with a refreshed token.</summary>
    public Func<Metadata>? HeadersFactory { get; init; }
}