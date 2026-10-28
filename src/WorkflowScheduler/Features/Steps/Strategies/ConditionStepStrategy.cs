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

    public StepExecutionResult Execute(ProcessInstance processInstance,
        StepInstance stepInstance,
        StepDefinition stepDefinition)
    {
        var condition = (StepMetadata.Condition)stepDefinition.ParsedMetadata;
        // Strategies are scoped to one sequential command execution; retain its immutable parsed input.
        if (!ReferenceEquals(_inputData, processInstance.InputData))
        {
            _input = JsonElement.Parse(processInstance.InputData);
            _inputData = processInstance.InputData;
        }

        var result = Evaluate(_input, condition);
        var nextStepId = result ? condition.TrueNextStepId : condition.FalseNextStepId;

        return new StepExecutionResult(StepExecutionState.Completed, nextStepId);
    }

    private static bool Evaluate(JsonElement input, StepMetadata.Condition condition)
    {
        if (input.ValueKind != JsonValueKind.Object ||
            !input.TryGetProperty(condition.InputKey, out var element))
            return false;

        var value = element.ValueKind == JsonValueKind.String
            ? element.GetString() ?? string.Empty
            : element.GetRawText();

        return condition.Operator switch
        {
            "eq" => value == condition.Value,
            "ne" => value != condition.Value,
            _ => CompareNumeric(value, condition.Operator, condition.NumericValue!.Value)
        };
    }

    private static bool CompareNumeric(string left, string op, decimal right)
    {
        if (!decimal.TryParse(left, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            return false;

        return op switch
        {
            "gt" => value > right,
            "lt" => value < right,
            "gte" => value >= right,
            "lte" => value <= right,
            _ => false
        };
    }
}