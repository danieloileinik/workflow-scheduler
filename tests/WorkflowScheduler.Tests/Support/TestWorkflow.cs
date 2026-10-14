namespace WorkflowScheduler.Tests.Support;

internal static class TestWorkflow
{
    public static StepDefinitionDto Call(Guid? next = null, Guid? compensation = null, string service = "billing")
    {
        return new StepDefinitionDto(
            Guid.NewGuid(),
            "Call",
            StepType.Call,
            $$"""{"serviceName":"{{service}}","command":"Execute"}""",
            next,
            compensation);
    }

    public static StepDefinitionDto Wait(Guid? next = null)
    {
        return new StepDefinitionDto(
            Guid.NewGuid(),
            "Wait",
            StepType.WaitEvent,
            """{"eventName":"Approved"}""",
            next,
            null);
    }

    public static RegisterProcessDefinitionCommand Register(params StepDefinitionDto[] steps)
    {
        return new RegisterProcessDefinitionCommand("workflow-" + Guid.NewGuid(), 1, steps);
    }
}