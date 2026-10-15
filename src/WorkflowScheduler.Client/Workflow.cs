using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using WorkflowScheduler.Contracts;

namespace WorkflowScheduler.Client;

public static class Workflow
{
    public static WorkflowBuilder Define(string name, int version = 1)
    {
        Guard.Text(name, nameof(name), 256);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        return new WorkflowBuilder(new WorkflowBuilder.DefinitionState(name, version), true);
    }
}

public enum WorkflowStepKind
{
    Call,
    WaitEvent,
    Condition
}

public enum WorkflowComparison
{
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual
}

public sealed record WorkflowStepDefinition(
    Guid Id,
    string Name,
    WorkflowStepKind Kind,
    string Metadata,
    Guid? NextStepId,
    Guid? CompensationStepId);

public sealed class WorkflowDefinition
{
    internal WorkflowDefinition(string name, int version, WorkflowStepDefinition[] steps)
    {
        Name = name;
        Version = version;
        Steps = Array.AsReadOnly(steps);
    }

    public string Name { get; }
    public int Version { get; }
    public IReadOnlyList<WorkflowStepDefinition> Steps { get; }
}

/// <summary>Builds sequential workflows with conditional branches and terminal compensations.</summary>
public sealed class WorkflowBuilder
{
    private static readonly JsonSerializerOptions MetadataOptions = new(JsonSerializerDefaults.Web);
    private readonly DefinitionState _definition;
    private readonly bool _root;
    private readonly List<MutableStep> _tails = [];

    private bool _definitionBranchClosed;
    private MutableStep? _first;
    private bool _insideBranch;
    private MutableStep? _last;

    internal WorkflowBuilder(DefinitionState definition, bool root)
    {
        _definition = definition;
        _root = root;
    }

    public WorkflowBuilder Call(string service, string command, string? name = null)
    {
        ValidateCall(service, command);
        Append(NewStep(name ?? command, WorkflowStepKind.Call, new { serviceName = service, command }));
        return this;
    }

    public WorkflowBuilder WaitFor(string eventName, string? name = null)
    {
        Guard.Text(eventName, nameof(eventName), WorkflowProtocol.MaxEventNameLength);
        Append(NewStep(name ?? eventName, WorkflowStepKind.WaitEvent, new { eventName }));
        return this;
    }

    public WorkflowBuilder CompensateWith(string service, string command, string? name = null)
    {
        EnsureEditable();
        if (_last is not { Kind: WorkflowStepKind.Call })
            throw new InvalidOperationException("CompensateWith must follow a Call step.");
        if (_last.CompensationStepId is not null)
            throw new InvalidOperationException("This Call step already has a compensation.");
        ValidateCall(service, command);
        var compensation = NewStep(
            name ?? command,
            WorkflowStepKind.Call,
            new { serviceName = service, command });
        compensation.IsCompensation = true;
        _last.CompensationStepId = compensation.Id;
        return this;
    }

    public WorkflowBuilder Condition(string inputKey,
        WorkflowComparison comparison,
        decimal value,
        Action<WorkflowBuilder> whenTrue,
        Action<WorkflowBuilder> whenFalse,
        string? name = null)
    {
        return Condition(inputKey, comparison, value.ToString(CultureInfo.InvariantCulture), whenTrue, whenFalse, name);
    }

    public WorkflowBuilder Condition(string inputKey,
        WorkflowComparison comparison,
        string value,
        Action<WorkflowBuilder> whenTrue,
        Action<WorkflowBuilder> whenFalse,
        string? name = null)
    {
        EnsureEditable();
        Guard.Text(inputKey, nameof(inputKey));
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(whenTrue);
        ArgumentNullException.ThrowIfNull(whenFalse);
        var op = comparison switch
        {
            WorkflowComparison.Equal => "eq",
            WorkflowComparison.NotEqual => "ne",
            WorkflowComparison.GreaterThan => "gt",
            WorkflowComparison.LessThan => "lt",
            WorkflowComparison.GreaterThanOrEqual => "gte",
            WorkflowComparison.LessThanOrEqual => "lte",
            _ => throw new ArgumentOutOfRangeException(nameof(comparison))
        };
        if (comparison is not (WorkflowComparison.Equal or WorkflowComparison.NotEqual)
            && !decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
            throw new ArgumentException("Numeric comparisons require an invariant decimal value.", nameof(value));

        var condition = NewStep(name ?? $"Check {inputKey}", WorkflowStepKind.Condition, new { });
        Append(condition);
        var yes = new WorkflowBuilder(_definition, false);
        var no = new WorkflowBuilder(_definition, false);
        _insideBranch = true;
        try
        {
            whenTrue(yes);
            whenFalse(no);
        }
        catch
        {
            _definition.Faulted = true;
            throw;
        }
        finally
        {
            _insideBranch = false;
            yes._definitionBranchClosed = no._definitionBranchClosed = true;
        }

        if (yes._first is null || no._first is null)
        {
            _definition.Faulted = true;
            throw new InvalidOperationException("Both condition branches must contain at least one step.");
        }

        condition.Metadata = JsonSerializer.Serialize(
            new
            {
                inputKey, @operator = op, value,
                trueNextStepId = yes._first.Id, falseNextStepId = no._first.Id
            },
            MetadataOptions);
        _tails.Clear();
        _tails.AddRange(yes._tails);
        _tails.AddRange(no._tails);
        return this;
    }

    public WorkflowDefinition Build()
    {
        EnsureEditable();
        if (!_root) throw new InvalidOperationException("Build is only available on the root workflow builder.");
        if (_first is null) throw new InvalidOperationException("A workflow must contain at least one step.");
        var steps = _definition
            .Steps
            .Where(step => !step.IsCompensation)
            .Concat(_definition.Steps.Where(step => step.IsCompensation))
            .Select(step => new WorkflowStepDefinition(
                step.Id,
                step.Name,
                step.Kind,
                step.Metadata,
                step.NextStepId,
                step.CompensationStepId))
            .ToArray();
        return new WorkflowDefinition(_definition.Name, _definition.Version, steps);
    }

    private void EnsureEditable()
    {
        if (_definition.Faulted)
            throw new InvalidOperationException("A condition failed to build; create a new workflow builder.");
        if (_insideBranch || _definitionBranchClosed)
            throw new InvalidOperationException("Modify only the branch builder supplied to the current callback.");
    }

    private static void ValidateCall(string service, string command)
    {
        Guard.Text(service, nameof(service));
        Guard.Text(command, nameof(command), WorkflowProtocol.MaxCommandLength);
        if (!WorkflowProtocol.IsValidTopic(WorkflowProtocol.CommandTopic(service)))
            throw new ArgumentException("Service name must form a valid Kafka command topic.", nameof(service));
    }

    private MutableStep NewStep(string name, WorkflowStepKind kind, object metadata)
    {
        EnsureEditable();
        Guard.Text(name, nameof(name), 256);
        // Includes the definition identity and sequence, so versions cannot collide and rebuilding is stable.
        var identity = JsonSerializer.SerializeToUtf8Bytes(
            new { _definition.Name, _definition.Version, Position = _definition.Steps.Count });
        var id = new Guid(SHA256.HashData(identity).AsSpan(0, 16));
        var step = new MutableStep(id, name, kind, JsonSerializer.Serialize(metadata, MetadataOptions));
        _definition.Steps.Add(step);
        return step;
    }

    private void Append(MutableStep step)
    {
        _first ??= step;
        foreach (var previous in _tails) previous.NextStepId = step.Id;
        _tails.Clear();
        _tails.Add(step);
        _last = step;
    }

    internal sealed class DefinitionState(string name, int version)
    {
        public string Name { get; } = name;
        public int Version { get; } = version;
        public List<MutableStep> Steps { get; } = [];
        public bool Faulted { get; set; }
    }

    internal sealed class MutableStep(Guid id, string name, WorkflowStepKind kind, string metadata)
    {
        public Guid Id { get; } = id;
        public string Name { get; } = name;
        public WorkflowStepKind Kind { get; } = kind;
        public string Metadata { get; set; } = metadata;
        public Guid? NextStepId { get; set; }
        public Guid? CompensationStepId { get; set; }
        public bool IsCompensation { get; set; }
    }
}
