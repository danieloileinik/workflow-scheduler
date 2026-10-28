using System.Globalization;
using System.Text.Json;
using WorkflowScheduler.Contracts.Grpc;

namespace WorkflowScheduler.Domain.Steps;

public abstract record StepMetadata
{
    public static StepMetadata Parse(StepType type, string json)
    {
        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            throw new FormatException();

        var properties = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);

        if (document.RootElement.EnumerateObject().Any(property => !properties.TryAdd(property.Name, property.Value)))
            throw new FormatException();

        string RequiredString(string name, bool allowEmpty = false)
        {
            if (!properties.TryGetValue(name, out var value) || value.ValueKind != JsonValueKind.String)
                throw new FormatException();

            var text = value.GetString()!;

            return allowEmpty || !string.IsNullOrWhiteSpace(text) ? text : throw new FormatException();
        }

        switch (type)
        {
            case StepType.Call:
                var service = RequiredString("ServiceName");
                var command = RequiredString("Command");
                if (!WorkflowProtocol.IsValidTopic(WorkflowProtocol.CommandTopic(service))
                    || command.Length > WorkflowProtocol.MaxCommandLength)
                    throw new FormatException();
                return new Call(service, command);

            case StepType.WaitEvent:
                var eventName = RequiredString("EventName");
                return eventName.Length > WorkflowProtocol.MaxEventNameLength
                    ? throw new FormatException()
                    : new WaitEvent(eventName);

            case StepType.Condition:
                var inputKey = RequiredString("InputKey");
                var op = RequiredString("Operator");
                var value = RequiredString("Value", true);
                if (op is not ("eq" or "ne" or "gt" or "lt" or "gte" or "lte"))
                    throw new FormatException();

                decimal? numericValue = null;
                if (op is not ("eq" or "ne"))
                {
                    if (!decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var number))
                        throw new FormatException();

                    numericValue = number;
                }

                return new Condition(
                    inputKey,
                    value,
                    op,
                    Guid.Parse(RequiredString("TrueNextStepId")),
                    Guid.Parse(RequiredString("FalseNextStepId")),
                    numericValue);

            default:
                throw new FormatException();
        }
    }

    public sealed record Call(string ServiceName, string Command) : StepMetadata;

    public sealed record WaitEvent(string EventName) : StepMetadata;

    public sealed record Condition(
        string InputKey,
        string Value,
        string Operator,
        Guid TrueNextStepId,
        Guid FalseNextStepId,
        decimal? NumericValue) : StepMetadata;
}