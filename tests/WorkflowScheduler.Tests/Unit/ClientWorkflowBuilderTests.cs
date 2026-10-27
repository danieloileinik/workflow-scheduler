using System.Globalization;
using System.Text.Json;
using WorkflowScheduler.Client;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class ClientWorkflowBuilderTests
{
    [Fact]
    public void SequentialDefinition_LinksForwardStepsAndKeepsCompensationOffThePath()
    {
        var workflow = Workflow
            .Define("order")
            .Call("inventory", "Reserve")
            .CompensateWith("inventory", "Release")
            .WaitFor("Approved")
            .Call("shipping", "Ship")
            .Build();

        Assert.Equal(4, workflow.Steps.Count);

        var reserve = workflow.Steps[0];
        var wait = workflow.Steps[1];
        var ship = workflow.Steps[2];
        var undo = workflow.Steps[3];
        Assert.Equal(wait.Id, reserve.NextStepId);
        Assert.Equal(ship.Id, wait.NextStepId);
        Assert.Equal(undo.Id, reserve.CompensationStepId);
        Assert.Null(ship.NextStepId);
        Assert.Null(undo.NextStepId);
        Assert.Null(undo.CompensationStepId);
        AssertValidOnServer(workflow);
    }

    [Fact]
    public void NestedBranches_JoinAtTheNextForwardStepAndPassServerValidation()
    {
        var workflow = Workflow
            .Define("branch")
            .Condition(
                "amount",
                WorkflowComparison.GreaterThan,
                100m,
                yes => yes.Condition(
                    "vip",
                    WorkflowComparison.Equal,
                    "true",
                    vip => vip.Call("billing", "Vip"),
                    regular => regular.WaitFor("Review")),
                no => no.Call("billing", "Small"))
            .Call("shipping", "Ship")
            .Build();

        var join = workflow.Steps.Last();
        Assert.All(
            workflow.Steps.Where(step => step.Kind != WorkflowStepKind.Condition && step != join),
            step => Assert.Equal(join.Id, step.NextStepId));
        AssertValidOnServer(workflow);
    }

    [Theory]
    [InlineData(WorkflowComparison.Equal, "eq")]
    [InlineData(WorkflowComparison.NotEqual, "ne")]
    [InlineData(WorkflowComparison.GreaterThan, "gt")]
    [InlineData(WorkflowComparison.LessThan, "lt")]
    [InlineData(WorkflowComparison.GreaterThanOrEqual, "gte")]
    [InlineData(WorkflowComparison.LessThanOrEqual, "lte")]
    public void Conditions_UseSupportedOperatorsAndInvariantValues(WorkflowComparison comparison, string op)
    {
        var previous = CultureInfo.CurrentCulture;

        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ru-RU");

            var workflow = Workflow
                .Define("numeric")
                .Condition(
                    "amount",
                    comparison,
                    100.50m,
                    yes => yes.Call("billing", "High"),
                    no => no.Call("billing", "Low"))
                .Build();
            using var metadata = JsonDocument.Parse(workflow.Steps[0].Metadata);
            Assert.Equal(op, metadata.RootElement.GetProperty("operator").GetString());
            Assert.Equal("100.50", metadata.RootElement.GetProperty("value").GetString());
            AssertValidOnServer(workflow);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void RebuildingSameVersion_IsStableAndDifferentDefinitionsHaveDistinctStepIds()
    {
        var first = Build("order", 1);
        Assert.Equal(first.Steps, Build("order", 1).Steps);
        Assert.Empty(first.Steps.Select(step => step.Id).Intersect(Build("order", 2).Steps.Select(step => step.Id)));
        Assert.Empty(first.Steps.Select(step => step.Id).Intersect(Build("another", 1).Steps.Select(step => step.Id)));

        return;

        static WorkflowDefinition Build(string name, int version)
        {
            return Workflow
                .Define(name, version)
                .Call("inventory", "Reserve")
                .CompensateWith("inventory", "Release")
                .WaitFor("Approved")
                .Build();
        }
    }

    [Fact]
    public void Build_ReturnsImmutableSnapshotWhileBuilderCanContinue()
    {
        var builder = Workflow.Define("snapshot").Call("billing", "Charge");
        var snapshot = builder.Build();
        var expanded = builder.WaitFor("Approved").Build();
        Assert.Single(snapshot.Steps);
        Assert.Null(snapshot.Steps[0].NextStepId);
        Assert.Equal(expanded.Steps[1].Id, expanded.Steps[0].NextStepId);
        Assert.Throws<NotSupportedException>(() => ((IList<WorkflowStepDefinition>)snapshot.Steps).Clear());
    }

    [Fact]
    public void InvalidFluentUsage_FailsBeforeRegistration()
    {
        Assert.Throws<ArgumentException>(() => Workflow.Define(" "));
        Assert.Throws<ArgumentOutOfRangeException>(() => Workflow.Define("order", 0));
        Assert.Throws<InvalidOperationException>(() => Workflow.Define("empty").Build());
        Assert.Throws<InvalidOperationException>(() =>
            Workflow.Define("wait").WaitFor("Ready").CompensateWith("billing", "Undo"));
        Assert.Throws<InvalidOperationException>(() => Workflow
            .Define("double")
            .Call("billing", "Charge")
            .CompensateWith("billing", "Undo")
            .CompensateWith("billing", "UndoAgain"));
        Assert.Throws<ArgumentException>(() => Workflow
            .Define("numeric")
            .Condition(
                "amount",
                WorkflowComparison.GreaterThan,
                "not-a-number",
                yes => yes.Call("billing", "High"),
                no => no.Call("billing", "Low")));
    }

    [Fact]
    public void FailedOrEmptyBranch_CannotLeaveARegisterablePartialGraph()
    {
        var empty = Workflow.Define("empty-branch");
        Assert.Throws<InvalidOperationException>(() => empty.Condition(
            "x",
            WorkflowComparison.Equal,
            "1",
            _ => { },
            no => no.Call("billing", "No")));
        Assert.Throws<InvalidOperationException>(empty.Build);

        var faulty = Workflow.Define("faulty-branch");
        Assert.Throws<ArgumentException>(() => faulty.Condition(
            "x",
            WorkflowComparison.Equal,
            "1",
            yes => yes.Call("", "Yes"),
            no => no.Call("billing", "No")));
        Assert.Throws<InvalidOperationException>(faulty.Build);
    }

    [Fact]
    public void CapturedBranch_CannotBeChangedAfterItsCallbackCompletes()
    {
        WorkflowBuilder? captured = null;
        var definition = Workflow
            .Define("closed-branch")
            .Condition(
                "x",
                WorkflowComparison.Equal,
                "1",
                yes =>
                {
                    captured = yes;
                    yes.Call("billing", "Yes");
                },
                no => no.Call("billing", "No"))
            .Build();
        Assert.Throws<InvalidOperationException>(() => captured!.Call("billing", "Late"));
        AssertValidOnServer(definition);
    }

    private static void AssertValidOnServer(WorkflowDefinition definition)
    {
        var command = new RegisterProcessDefinitionCommand(
            definition.Name,
            definition.Version,
            [
                .. definition
                    .Steps
                    .Select(step => new StepDefinitionDto(
                        step.Id,
                        step.Name,
                        step.Kind switch
                        {
                            WorkflowStepKind.Call => StepType.Call,
                            WorkflowStepKind.WaitEvent => StepType.WaitEvent,
                            _ => StepType.Condition
                        },
                        step.Metadata,
                        step.NextStepId,
                        step.CompensationStepId))
            ]);
        var result = new RegisterProcessDefinitionValidator().Validate(command);
        Assert.True(result.IsValid, result.ToString());
    }
}