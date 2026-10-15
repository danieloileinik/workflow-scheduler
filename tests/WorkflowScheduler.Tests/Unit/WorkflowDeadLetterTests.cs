using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WorkflowScheduler.Contracts;
using WorkflowScheduler.Worker;

namespace WorkflowScheduler.Tests.Unit;

[Trait("Category", "Unit")]
public class WorkflowDeadLetterTests
{
    [Fact]
    public void SmallMessage_PreservesCompletePayloadAndKey()
    {
        var letter = new WorkflowDeadLetter("commands", 1, 42, "key", "{bad-json", "Invalid JSON");

        var restored = JsonSerializer.Deserialize<WorkflowDeadLetter>(letter.Serialize(8192));

        Assert.Equal(letter, restored);
        var (topic, partition, offset, key, payload, error) = restored!;
        Assert.Equal(
            ("commands", 1, 42L, "key", "{bad-json", "Invalid JSON"),
            (topic, partition, offset, key, payload, error));
    }

    [Theory]
    [InlineData(8192)]
    [InlineData(WorkflowProtocol.MaxKafkaValueBytes)]
    public void OversizedMessage_FitsLimitAndPreservesSourceIdentityAndPayloadFingerprint(int limit)
    {
        var payload = new string('"', 700_000);
        var letter = new WorkflowDeadLetter(
            "commands",
            1,
            42,
            new string('"', 200_000),
            payload,
            new string('"', 10_000));

        var json = letter.Serialize(limit);
        var restored = JsonSerializer.Deserialize<WorkflowDeadLetter>(json)!;

        Assert.True(Encoding.UTF8.GetByteCount(json) <= limit);
        Assert.Equal("commands", restored.Topic);
        Assert.Equal(1, restored.Partition);
        Assert.Equal(42, restored.Offset);
        Assert.True(restored.PayloadTruncated);
        Assert.True(restored.KeyTruncated);
        Assert.StartsWith(restored.Payload!, payload);
        Assert.Equal(Encoding.UTF8.GetByteCount(payload), restored.OriginalPayloadBytes);
        Assert.Equal(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(payload))), restored.PayloadSha256);
    }
}