namespace WorkflowScheduler.Tests.Support;

[CollectionDefinition(Name)]
public sealed class InfrastructureCollection : ICollectionFixture<TestInfrastructure>
{
    public const string Name = "PostgreSQL, Redis and Kafka";
}