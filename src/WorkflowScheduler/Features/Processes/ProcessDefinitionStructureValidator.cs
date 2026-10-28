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
        if (definition.Steps.Count == 0)
            return;

        var steps = new Dictionary<Guid, StepDefinition>(definition.Steps.Count);
        var compensationIds = new HashSet<Guid>();
        var automaticEdges = new Dictionary<Guid, List<Guid>>();
        var degree = new Dictionary<Guid, int>();

        foreach (var step in definition.Steps)
        {
            if (!steps.TryAdd(step.Id, step))
            {
                context.AddFailure("Steps", "Step ids must be unique");
                continue;
            }

            if (step.CompensationStepId is { } compensation)
                compensationIds.Add(compensation);

            if (step.Type == StepType.Condition)
            {
                automaticEdges.Add(step.Id, []);
                degree.Add(step.Id, 0);
            }
        }

        if (steps.Count != definition.Steps.Count)
            return;

        var stepsWithMissingReferences = 0;
        var compensationOnForwardPath = compensationIds.Contains(definition.Steps[0].Id);

        foreach (var step in definition.Steps)
        {
            var referencesValid = true;
            if (step.NextStepId is { } next)
                referencesValid = CheckReference(step, next);

            if (step.CompensationStepId is { } compensation
                && (step.Type != StepType.Call || !steps.TryGetValue(compensation, out var target)
                                               || target.Type != StepType.Call || target.NextStepId is not null
                                               || target.CompensationStepId is not null || target.Id == step.Id))
                context.AddFailure("Steps", "Compensation must reference a separate terminal Call step");

            try
            {
                if (step.ParsedMetadata is StepMetadata.Condition condition)
                {
                    referencesValid &= CheckReference(step, condition.TrueNextStepId);
                    referencesValid &= CheckReference(step, condition.FalseNextStepId);
                }
            }
            catch (Exception ex) when (ex is JsonException or FormatException or InvalidOperationException
                                           or ArgumentNullException)
            {
                context.AddFailure("Steps", $"Invalid metadata for step {step.Name}");
            }

            if (!referencesValid)
                stepsWithMissingReferences++;
        }

        for (var index = 0; index < stepsWithMissingReferences; index++)
            context.AddFailure("Steps", "All step references must belong to this definition");

        if (stepsWithMissingReferences > 0)
            return;

        // Only Condition-only cycles spin without yielding. Call/WaitEvent loops are valid.
        var ready = new Queue<Guid>(degree.Where(entry => entry.Value == 0).Select(entry => entry.Key));
        var count = 0;

        while (ready.TryDequeue(out var id))
        {
            count++;

            foreach (var next in automaticEdges[id])
                if (--degree[next] == 0)
                    ready.Enqueue(next);
        }

        if (count != automaticEdges.Count)
            context.AddFailure("Steps", "Automatic condition chains must be acyclic");

        if (compensationOnForwardPath)
            context.AddFailure("Steps", "Compensation steps cannot be on the forward execution path");
        return;

        bool CheckReference(StepDefinition source, Guid targetId)
        {
            if (!steps.TryGetValue(targetId, out var target))
                return false;

            compensationOnForwardPath |= compensationIds.Contains(targetId);

            if (source.Type == StepType.Condition && target.Type == StepType.Condition)
            {
                automaticEdges[source.Id].Add(targetId);
                degree[targetId]++;
            }

            return true;
        }
    }
}