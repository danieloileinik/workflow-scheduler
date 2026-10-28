using FluentValidation;
using FluentValidation.Results;
using MediatR;
using ValidationException = FluentValidation.ValidationException;

namespace WorkflowScheduler.Common.Behaviors;

public class ValidationBehavior<TRequest, TResponse>(IEnumerable<IValidator<TRequest>> validators)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken ct)
    {
        List<ValidationFailure>? failures = null;

        foreach (var validator in validators)
        {
            var result = validator.Validate(request);
            if (result.Errors.Count == 0)
                continue;

            failures ??= [];
            failures.AddRange(result.Errors);
        }

        if (failures is not null)
            throw new ValidationException(failures);

        return await next(ct);
    }
}
