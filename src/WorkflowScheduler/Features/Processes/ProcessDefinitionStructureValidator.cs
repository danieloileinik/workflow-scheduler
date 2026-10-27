using System.Text.Json;
using FluentValidation;
using WorkflowScheduler.Domain.Processes;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Features.Processes;

public class ProcessDefinitionStructureValidator : AbstractValidator<ProcessDefinition>
{
    public ProcessDefinitionStructureValidator()
    {
        RuleFor(x => x.Name).NotEmpty().MaximumLength(256);
        RuleFor(x => x.Version).GreaterThan(0);
        RuleFor(x => x.Steps).NotEmpty();
        RuleForEach(x => x.Steps)
            .ChildRules(step =>
            {
                step.RuleFor(s => s.Id).NotEmpty();
                step.RuleFor(s => s.Name).NotEmpty().MaximumLength(256);
                step.RuleFor(s => s.Type).IsInEnum();
            });
        RuleFor(x => x).Custom(ValidateGraph);
    }

    private static void ValidateGraph(ProcessDefinition definition,
        ValidationContext<ProcessDefinition> context)
    {
        var suppliedSteps = definition.Steps.ToList();
        if (suppliedSteps.Count == 0)
            return;

        var steps = new Dictionary<Guid, StepDefinition>();

        foreach (var step in definition.Steps)
            if (!steps.TryAdd(step.Id, step))
                context.AddFailure("Steps", "Step ids must be unique");

        if (steps.Count != suppliedSteps.Count)
            return;

        var edges = steps.Keys.ToDictionary(id => id, _ => new List<Guid>());

        foreach (var step in definition.Steps)
        {
            if (step.NextStepId is Guid next)
                edges[step.Id].Add(next);
            if (step.CompensationStepId is Guid compensation)
                if (step.Type != StepType.Call || !steps.TryGetValue(compensation, out var target)
                                               || target.Type != StepType.Call || target.NextStepId is not null
                                               || target.CompensationStepId is not null || target.Id == step.Id)
                    context.AddFailure("Steps", "Compensation must reference a separate terminal Call step");

            try
            {
                if (step.ParsedMetadata is StepMetadata.Condition condition)
                {
                    edges[step.Id].Add(condition.TrueNextStepId);
                    edges[step.Id].Add(condition.FalseNextStepId);
                }
            }
            catch (Exception ex) when (ex is JsonException or FormatException or InvalidOperationException
                                           or ArgumentNullException)
            {
                context.AddFailure("Steps", $"Invalid metadata for step {step.Name}");
            }
        }

        foreach (var targets in edges.Values)
            if (targets.Any(id => !steps.ContainsKey(id)))
                context.AddFailure("Steps", "All step references must belong to this definition");

        if (edges.Values.SelectMany(x => x).Any(id => !steps.ContainsKey(id)))
            return;
        // Only Condition-only cycles spin without yielding. Call/WaitEvent loops are valid.
        // Kahn's algorithm checks the induced automatic graph without recursion.
        var automaticEdges = steps.Keys.ToDictionary(
            id => id,
            id =>
                steps[id].Type == StepType.Condition
                    ? [.. edges[id].Where(next => steps[next].Type == StepType.Condition)]
                    : new List<Guid>());
        var degree = steps.Keys.ToDictionary(id => id, _ => 0);

        foreach (var id in automaticEdges.Values.SelectMany(x => x))
            degree[id]++;

        var ready = new Queue<Guid>(degree.Where(x => x.Value == 0).Select(x => x.Key));
        var count = 0;

        while (ready.TryDequeue(out var id))
        {
            count++;

            foreach (var next in automaticEdges[id].Where(next => --degree[next] == 0))
                ready.Enqueue(next);
        }

        if (count != steps.Count)
            context.AddFailure("Steps", "Automatic condition chains must be acyclic");

        var compensationIds = definition
            .Steps
            .Where(x => x.CompensationStepId.HasValue)
            .Select(x => x.CompensationStepId!.Value)
            .ToHashSet();
        if (compensationIds.Contains(suppliedSteps[0].Id)
            || edges.Values.SelectMany(x => x).Any(compensationIds.Contains))
            context.AddFailure("Steps", "Compensation steps cannot be on the forward execution path");
    }

}
