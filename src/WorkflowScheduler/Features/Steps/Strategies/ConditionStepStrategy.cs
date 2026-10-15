using System.Globalization;
using System.Text.Json;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Steps.Strategies;

public class ConditionStepStrategy : IStepStrategy
{
    public StepType StepType => StepType.Condition;

    public Task<StepExecutionResult> ExecuteAsync(ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition,
        CancellationToken ct)
    {
        var condition = JsonSerializer.Deserialize<ConditionMetadata>(stepDefinition.Metadata, StepMetadata.Options);
        var result = Evaluate(processInstance.InputData, condition);
        var nextStepId = result ? condition.TrueNextStepId : condition.FalseNextStepId;
        return Task.FromResult(new StepExecutionResult(StepExecutionState.Completed, nextStepId));
    }

    private static bool Evaluate(string inputData, ConditionMetadata condition)
    {
        using var doc = JsonDocument.Parse(inputData);
        if (doc.RootElement.ValueKind != JsonValueKind.Object ||
            !doc.RootElement.TryGetProperty(condition.InputKey, out var element)) return false;

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