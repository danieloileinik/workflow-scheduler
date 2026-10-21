using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace WorkflowScheduler.Common;

public static class AdvisoryLock
{
    public static long ForProcess(Guid processInstanceId)
    {
        return FromKey($"process:{processInstanceId}");
    }

    public static long FromKey(string key)
    {
        return BinaryPrimitives.ReadInt64BigEndian(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
    }
}