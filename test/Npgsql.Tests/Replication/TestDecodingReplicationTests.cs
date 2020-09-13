using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.TestDecoding;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class TestDecodingReplicationTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {
        [Test]
        public Task CreateReplicationSlot()
            => SafeReplicationTest(nameof(CreateReplicationSlot),
                async (slotName, _) =>
                {
                    await using var rc = await OpenReplicationConnectionAsync();
                    var options = await rc.CreateReplicationSlot(slotName);

                    await using var c = await OpenConnectionAsync();
                    using var cmd =
                        new NpgsqlCommand($"SELECT * FROM pg_replication_slots WHERE slot_name = '{options.SlotName}'",
                            c);
                    await using var reader = await cmd.ExecuteReaderAsync();

                    Assert.That(reader.Read, Is.True);
                    Assert.That(reader.GetFieldValue<string>(reader.GetOrdinal("slot_type")), Is.EqualTo("logical"));
                    Assert.That(reader.GetFieldValue<string>(reader.GetOrdinal("plugin")), Is.EqualTo("test_decoding"));
                    Assert.That(reader.Read, Is.False);
                });
    }
}
