using Microsoft.EntityFrameworkCore.Diagnostics;

namespace WorkflowScheduler.Tests.Support;

internal sealed class StepMaterializationProbe : IMaterializationInterceptor
{
    public List<StepInstance> Steps { get; } = [];

    public object InitializedInstance(MaterializationInterceptionData materializationData, object entity)
    {
        if (entity is StepInstance step)
            Steps.Add(step);

        return entity;
    }
}