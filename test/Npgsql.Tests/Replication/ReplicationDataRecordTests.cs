using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput.Messages;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    [TestFixture(ReplicationDataMode.DefaultReplicationDataMode, Buffering.Unbuffered)]
    [TestFixture(ReplicationDataMode.BinaryReplicationDataMode, Buffering.Unbuffered)]
    [TestFixture(ReplicationDataMode.DefaultReplicationDataMode, Buffering.Buffered)]
    [TestFixture(ReplicationDataMode.BinaryReplicationDataMode, Buffering.Buffered)]
    public class ReplicationDataRecordTests : PgOutputReplicationTestBase
    {

        [Test(Description = "Tests Logical Replication Protocol data for the smallint type")]
        public Task Smallint()
            => SafePgOutputReplicationTest(
                async (slotName, tableName, publicationName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    await c.ExecuteNonQueryAsync(@$"CREATE TABLE {tableName} (value1 smallint PRIMARY KEY, value2 smallint, value3 smallint);
                                                    CREATE PUBLICATION {publicationName} FOR TABLE {tableName};");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreatePgOutputReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync(@$"INSERT INTO {tableName} VALUES (1::smallint, 2::smallint, 3::smallint)");

                    using var streamingCts = new CancellationTokenSource();
                    var messages = SkipEmptyTransactions(rc.StartReplication(slot, GetOptions(publicationName), streamingCts.Token))
                        .GetAsyncEnumerator();

                    // Begin Transaction, Relation
                    await AssertTransactionStart(messages);
                    await NextMessage<RelationMessage>(messages);

                    // Insert first value
                    var insertMsg = await NextMessageBuffered<InsertMessage>(messages);
                    if (IsBinaryMode)
                    {
                        Assert.That(await insertMsg.NewRow.GetFieldValueAsync<short>(0, streamingCts.Token), Is.EqualTo(1));
                        Assert.That(insertMsg.NewRow.GetFieldValue<short>(1), Is.EqualTo(2));
                        Assert.That(insertMsg.NewRow.GetInt16(2), Is.EqualTo(3));
                    }
                    else
                    {
                        Assert.That(await insertMsg.NewRow.GetFieldValueAsync<string>(0, streamingCts.Token), Is.EqualTo("1"));
                        Assert.That(insertMsg.NewRow.GetFieldValue<string>(1), Is.EqualTo("2"));
                        Assert.That(insertMsg.NewRow.GetString(2), Is.EqualTo("3"));
                    }

                    // Commit Transaction
                    await AssertTransactionCommit(messages);
                    streamingCts.Cancel();
                    await AssertReplicationCancellation(messages);
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        async Task<T> NextMessageBuffered<T>(IAsyncEnumerator<PgOutputReplicationMessage> messages)
            where T : PgOutputReplicationMessage
        {
            var message = await NextMessage<T>(messages);
            return IsBuffered ? (T)message.Clone() : message;
        }

        public ReplicationDataRecordTests(ReplicationDataMode dataMode, Buffering buffering)
            : base(ProtocolVersionMode.ProtocolV1, dataMode, TransactionStreamingMode.DefaultTransactionMode)
            => IsBuffered = buffering == Buffering.Buffered;

        bool IsBuffered { get; }

        public enum Buffering
        {
            Unbuffered,
            Buffered
        }
    }
}
