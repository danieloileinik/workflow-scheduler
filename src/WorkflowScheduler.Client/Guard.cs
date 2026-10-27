namespace WorkflowScheduler.Client;

internal static class Guard
{
    public static void Id(Guid id, string name)
    {
        if (id == Guid.Empty)
            throw new ArgumentException("An identifier must not be empty.", name);
    }

    public static void Text(string value, string name, int maximumLength = int.MaxValue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);
        if (value.Length > maximumLength)
            throw new ArgumentException($"The value must not exceed {maximumLength} characters.", name);
    }
}