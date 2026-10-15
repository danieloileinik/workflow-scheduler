using System.Text.Json;

namespace WorkflowScheduler.Contracts;

public static class WorkflowProtocol
{
    public const int MaxEventNameLength = 250;
    public const int MaxCommandLength = 256;
    // Leave room for the Kafka record overhead and the command envelope.
    public const int MaxKafkaValueBytes = 990_000;
    public const int MaxInputStringBytes = 980_000;

    public static string CommandTopic(string serviceName) => "process.commands." + serviceName;

    public static bool IsValidTopic(string topic) =>
        !string.IsNullOrWhiteSpace(topic) && topic.Length <= 249 && topic is not ("." or "..")
        && topic.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-');

    public static bool InputFitsMessage(string input) =>
        JsonEncodedText.Encode(input).EncodedUtf8Bytes.Length <= MaxInputStringBytes;
}
