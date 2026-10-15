using System.Globalization;
using System.Text.Json;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class ConditionStepStrategy : IStepStrategy
{
    private JsonElement _input;
    private string? _inputData;
    public StepType StepType => StepType.Condition;

    public ValueTask<StepExecutionResult> ExecuteAsync(ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        var condition = JsonSerializer.Deserialize<ConditionMetadata>(stepDefinition.Metadata, StepMetadata.Options);
        // Strategies are scoped to one sequential command execution; retain its immutable parsed input.
        if (!ReferenceEquals(_inputData, processInstance.InputData))
        {
            _input = JsonElement.Parse(processInstance.InputData);
            _inputData = processInstance.InputData;
        }

        var result = Evaluate(_input, condition);
        var nextStepId = result ? condition.TrueNextStepId : condition.FalseNextStepId;
        return ValueTask.FromResult(new StepExecutionResult(StepExecutionState.Completed, nextStepId));
    }

    private static bool Evaluate(JsonElement input, ConditionMetadata condition)
    {
        if (input.ValueKind != JsonValueKind.Object ||
            !input.TryGetProperty(condition.InputKey, out var element)) return false;

        var value = element.ValueKind == JsonValueKind.String
            ? element.GetString() ?? string.Empty
            : element.GetRawText();

        return condition.Operator switch
        {
            "eq" => value == condition.Value,
            "ne" => value != condition.Value,
            _ => CompareNumeric(value, condition.Operator, condition.Value)
        };
    }

    private static bool CompareNumeric(string left, string op, string right)
    {
        if (!decimal.TryParse(left, NumberStyles.Float, CultureInfo.InvariantCulture, out var l) || !decimal.TryParse(
                right,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out var r)) return false;
        return op switch
        {
            "gt" => l > r,
            "lt" => l < r,
            "gte" => l >= r,
            "lte" => l <= r,
            _ => false
        };
    }

    private readonly record struct ConditionMetadata(
        string InputKey,
        string Value,
        string Operator,
        Guid TrueNextStepId,
        Guid FalseNextStepId);
}