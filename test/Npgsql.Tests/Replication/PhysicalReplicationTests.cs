using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.Physical;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class PhysicalReplicationTests : SafeReplicationTestBase<NpgsqlPhysicalReplicationConnection>
    {
        [Test]
        public Task CreateReplicationSlot()
            => SafeReplicationTest(nameof(CreateReplicationSlot),
                async (slotName, _) =>
                {
                    await using var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await using var c = await OpenConnectionAsync();
                    using var cmd =
                        new NpgsqlCommand($"SELECT * FROM pg_replication_slots WHERE slot_name = '{slot.SlotName}'",
                            c);
                    await using var reader = await cmd.ExecuteReaderAsync();

                    Assert.That(reader.Read, Is.True);
                    Assert.That(reader.GetFieldValue<string>(reader.GetOrdinal("slot_type")), Is.EqualTo("physical"));
                    Assert.That(reader.Read, Is.False);
                    await rc.DropReplicationSlot(slotName);
                });

        [Test]
        public Task PhysicalReplicationWithSlot()
            => SafeReplicationTest(nameof(PhysicalReplicationWithSlot),
                async (slotName, tableName) =>
                {
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, byte[] data)>();
                    var rc = await OpenReplicationConnectionAsync();
                    // We need to set reserveWal to true to make sure we get the WAL entries for the SQL we execute
                    // before starting replication
                    var slot = await rc.CreateReplicationSlot(slotName, reserveWal: true);
                    var info = await rc.IdentifySystem();

                    await using var c = await OpenConnectionAsync();
                    await c.ExecuteNonQueryAsync($"CREATE TABLE {tableName} (value text)");

                    for (var i = 1; i <= 10; i++)
                        await c.ExecuteNonQueryAsync($"INSERT INTO {tableName} VALUES ('Value {i}')");

                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach (var msg in await rc.StartReplication(slot, info.XLogPos))
                        {
                            using var memoryStream = new MemoryStream();
                            await msg.Data.CopyToAsync(memoryStream);
                            messages.Enqueue((msg.WalStart, msg.WalEnd, memoryStream.ToArray()));
                        }
                    });

                    // Make sure the replication thread has started populating our queue
                    while (messages.IsEmpty)
                        await Task.Delay(10);

                    // We can't assert a lot in physical replication.
                    // Since we're replicating database wide, other transactions
                    // possibly from system processes can interfere here, inserting
                    // additional messages, but more likely we'll get everything in one big chunk.
                    Assert.That(messages.TryDequeue(out var message), Is.True);
                    Assert.That(message.WalStart, Is.EqualTo(info.XLogPos));
                    Assert.That(message.WalEnd, Is.GreaterThan(message.WalStart));
                    Assert.That(message.data.Length, Is.GreaterThan(0));

                    await rc.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState)).EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName);
                });

        [Test]
        public async Task PhysicalReplicationWithoutSlot()
        {
            var messages =
                new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, byte[] data)>();
            var rc = await OpenReplicationConnectionAsync();
            var info = await rc.IdentifySystem();

            var replicationTask = Task.Run(async () =>
            {
                await foreach (var msg in await rc.StartReplication(info.XLogPos))
                {
                    using var memoryStream = new MemoryStream();
                    await msg.Data.CopyToAsync(memoryStream);
                    messages.Enqueue((msg.WalStart, msg.WalEnd, memoryStream.ToArray()));
                }
            });
            var tableName = "t_physicalreplicationwithoutslot_p";
            await using var c = await OpenConnectionAsync();
            await c.ExecuteNonQueryAsync($"CREATE TABLE {tableName} (value text)");
            try
            {
                for (var i = 1; i <= 10; i++)
                    await c.ExecuteNonQueryAsync($"INSERT INTO {tableName} VALUES ('Value {i}')");

                // Make sure the replication thread has started populating our queue
                while (messages.IsEmpty)
                    await Task.Delay(10);

                // We can't assert a lot in physical replication.
                // Since we're replicating database wide, other transactions
                // possibly from system processes can interfere here, inserting
                // additional messages, but more likely we'll get everything in one big chunk.
                Assert.That(messages.TryDequeue(out var message), Is.True);
                Assert.That(message.WalStart, Is.EqualTo(info.XLogPos));
                Assert.That(message.WalEnd, Is.GreaterThan(message.WalStart));
                Assert.That(message.data.Length, Is.GreaterThan(0));

                await rc.Cancel();
                Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                    .With.InnerException.InstanceOf<PostgresException>()
                    .And.InnerException.Property(nameof(PostgresException.SqlState))
                    .EqualTo(PostgresErrorCodes.QueryCanceled));
            }
            finally
            {
                await c.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
            }
        }
    }
}
