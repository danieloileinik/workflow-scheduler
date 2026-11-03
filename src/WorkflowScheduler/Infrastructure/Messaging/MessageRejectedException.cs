namespace WorkflowScheduler.Infrastructure.Messaging;

public sealed class MessageRejectedException(string message, Exception? innerException = null)
    : Exception(message, innerException);
