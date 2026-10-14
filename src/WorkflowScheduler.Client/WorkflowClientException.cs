using Grpc.Core;

namespace WorkflowScheduler.Client;

public sealed class WorkflowClientException : Exception
{
    internal WorkflowClientException(string operation, RpcException cause)
        : base($"{operation} failed ({cause.StatusCode}): {cause.Status.Detail}", cause)
    {
        Operation = operation;
        StatusCode = cause.StatusCode;
        Detail = cause.Status.Detail;
        Trailers = cause.Trailers;
    }

    public string Operation { get; }
    public StatusCode StatusCode { get; }
    public string Detail { get; }
    public Metadata Trailers { get; }
}