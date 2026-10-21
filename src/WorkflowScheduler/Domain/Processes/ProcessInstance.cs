using ErrorOr;
using WorkflowScheduler.Domain.Steps;

namespace WorkflowScheduler.Domain.Processes;

public class ProcessInstance(Guid id, Guid definitionId, string businessKey, string inputData)
{
    private readonly List<StepInstance> _steps = [];
    private int _nextStepSequence;

    public ProcessInstance(Guid definitionId, string businessKey, string inputData)
        : this(Guid.NewGuid(), definitionId, businessKey, inputData)
    {
    }

    public Guid Id { get; init; } = id;
    public Guid DefinitionId { get; init; } = definitionId;
    public string BusinessKey { get; init; } = businessKey;
    public string InputData { get; init; } = inputData;
    public ProcessStatus Status { get; private set; } = ProcessStatus.Created;
    public Guid? CurrentStepId { get; private set; }
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; private set; }
    public IReadOnlyList<StepInstance> Steps => _steps;

    public void RestoreStepSequence(int lastSequence)
    {
        _nextStepSequence = Math.Max(_nextStepSequence, checked(lastSequence + 1));
    }

    public ErrorOr<StepInstance> Start(Guid firstStepDefinitionId)
    {
        if (Status != ProcessStatus.Created) return ProcessErrors.InvalidTransition(Status, ProcessStatus.Running);
        var step = CreateStepInstance(firstStepDefinitionId);
        Status = ProcessStatus.Running;
        CurrentStepId = step.Id;
        UpdatedAt = DateTime.UtcNow;
        return step;
    }

    public ErrorOr<StepInstance> MoveToStep(Guid stepDefinitionId)
    {
        if (Status != ProcessStatus.Running) return ProcessErrors.InvalidTransition(Status, ProcessStatus.Running);
        var step = CreateStepInstance(stepDefinitionId);
        CurrentStepId = step.Id;
        UpdatedAt = DateTime.UtcNow;
        return step;
    }

    public ErrorOr<Success> Wait()
    {
        if (Status != ProcessStatus.Running) return ProcessErrors.InvalidTransition(Status, ProcessStatus.Waiting);
        Status = ProcessStatus.Waiting;
        UpdatedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<Success> Resume()
    {
        if (Status != ProcessStatus.Waiting) return ProcessErrors.InvalidTransition(Status, ProcessStatus.Running);
        Status = ProcessStatus.Running;
        UpdatedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<Success> Complete()
    {
        if (Status is not (ProcessStatus.Running or ProcessStatus.Waiting))
            return ProcessErrors.InvalidTransition(Status, ProcessStatus.Completed);
        Status = ProcessStatus.Completed;
        UpdatedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<Success> Fail()
    {
        if (Status is ProcessStatus.Completed or ProcessStatus.Cancelled)
            return ProcessErrors.InvalidTransition(Status, ProcessStatus.Failed);
        Status = ProcessStatus.Failed;
        UpdatedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public ErrorOr<Success> Cancel()
    {
        switch (Status)
        {
            case ProcessStatus.Completed:
                return ProcessErrors.AlreadyCompleted;
            case ProcessStatus.Cancelled:
                return ProcessErrors.AlreadyCancelled;
        }

        Status = ProcessStatus.Cancelled;
        UpdatedAt = DateTime.UtcNow;
        return Result.Success;
    }

    public StepInstance AddCompensationStep(Guid stepDefinitionId)
    {
        var step = new StepInstance(Id, stepDefinitionId);
        step.SetSequence(_nextStepSequence++, true);
        _steps.Add(step);
        return step;
    }

    public StepInstance? GetCurrentStepInstance()
    {
        return CurrentStepId is null ? null : _steps.FirstOrDefault(s => s.Id == CurrentStepId);
    }

    private StepInstance CreateStepInstance(Guid stepId)
    {
        var step = new StepInstance(Id, stepId);
        step.SetSequence(_nextStepSequence++);
        _steps.Add(step);
        return step;
    }
}