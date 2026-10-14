using System.Text.Json;
using FluentValidation;

namespace WorkflowScheduler.Features.Processes.Commands.StartProcess;

public class StartProcessValidator : AbstractValidator<StartProcessCommand>
{
    public StartProcessValidator()
    {
        RuleFor(x => x.IdempotencyKey).NotEmpty();
        RuleFor(x => x.DefinitionId).NotEmpty();
        RuleFor(x => x.BusinessKey).NotEmpty().MaximumLength(512);
        RuleFor(x => x.InputData).Must(ValidInput).WithMessage("InputData must be a JSON object");
    }

    private static bool ValidInput(string input)
    {
        try
        {
            using var doc = JsonDocument.Parse(input);
            return doc.RootElement.ValueKind == JsonValueKind.Object;
        }
        catch (Exception ex) when (ex is JsonException or ArgumentNullException)
        {
            return false;
        }
    }
}