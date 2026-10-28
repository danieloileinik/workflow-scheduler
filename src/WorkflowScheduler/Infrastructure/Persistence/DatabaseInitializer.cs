namespace WorkflowScheduler.Infrastructure.Persistence;

public static class DatabaseInitializer
{
    public static async Task InitializeAsync(AppDbContext db, CancellationToken ct = default)
    {
        await db.Database.EnsureCreatedAsync(ct);

        string[] resources = ["OutboxDelivery", "ExecutionSequence", "CallRecovery"];

        foreach (var resource in resources)
        {
            await using var script = typeof(DatabaseInitializer).Assembly.GetManifestResourceStream(
                $"WorkflowScheduler.Database.{resource}.sql")!;
            using var reader = new StreamReader(script);
            await db.Database.ExecuteSqlRawAsync(await reader.ReadToEndAsync(ct), ct);
        }
    }
}
