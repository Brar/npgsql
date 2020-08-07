using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.Raw;
using Npgsql.Replication.Physical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    [TestFixture(typeof(NpgsqlLogicalReplicationConnection))]
    [TestFixture(typeof(NpgsqlPhysicalReplicationConnection))]
    public class CommonReplicationTests<TConnection> : SafeReplicationTestBase<TConnection>
        where TConnection : NpgsqlReplicationConnection, new()
    {
        [Test(Description = "Tests whether our automated feedback thread prevents the backend from disconnecting due to wal_sender_timeout")]
        public Task ReplicationSurvivesPausesLongerThanWalSenderTimeout() =>
            SafeReplicationTest(nameof(ReplicationSurvivesPausesLongerThanWalSenderTimeout), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "10.0", "The SHOW command, which is required to run this test was added to the Streaming Replication Protocol in PostgreSQL 10");

                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await using var replConn = await OpenReplicationConnectionAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    ApplicationName = slotName
                });
                var walSenderTimeout = ParseTimespan(await replConn.Show("wal_sender_timeout"));
                var idInfo = await replConn.IdentifySystem();
                Console.WriteLine($"The server wal_sender_timeout is configured to {walSenderTimeout}");
                var walReceiverStatusInterval = TimeSpan.FromTicks(walSenderTimeout.Ticks / 2L);
                Console.WriteLine($"Setting {nameof(NpgsqlReplicationConnection)}.{nameof(NpgsqlReplicationConnection.WalReceiverStatusInterval)} to {walReceiverStatusInterval}");
                replConn.WalReceiverStatusInterval = walReceiverStatusInterval;
                var slot = await CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"INSERT INTO \"{tableName}\" (name) VALUES ('val1')");
                await using var enumerator = (await slot!.StartReplication(idInfo.XLogPos)).GetAsyncEnumerator();

                var delay = TimeSpan.FromTicks(checked(walSenderTimeout.Ticks * 2L));
                Console.WriteLine($"Going to sleep for {delay}");
                await Task.Delay(delay);

                // Begin Transaction, Insert, Commit Transaction
                for (var i = 0; i < 3; i++)
                {
                    Assert.That(await enumerator.MoveNextAsync(), Is.True);
                }
            });


        // In physical replication mode this test takes up to 5 Minutes on my system.
        [Test(Description = "Tests whether synchronous replication works the way it should.")]
        public Task SynchronousReplication() =>
            SafeReplicationTest(nameof(SynchronousReplication), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");

                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
");

                await using var replConn = await OpenReplicationConnectionAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    // This must be one of the configured synchronous_standby_names from postgresql.conf
                    ApplicationName = "npgsql_test_sync_standby",
                    // We need wal_sender_timeout to be at least twice checkpoint_timeout to avoid getting feedback requests
                    // from the backend in physical replication which makes this test fail, so we disable it for this test.
                    Options = "wal_sender_timeout=0"
                });
                var idInfo = await replConn.IdentifySystem();

                // Set WalReceiverStatusInterval to infinite so that the automated feedback doesn't interfere with
                // our manual feedback
                replConn.WalReceiverStatusInterval = Timeout.InfiniteTimeSpan;

                var slot = await CreateReplicationSlot(replConn, slotName);
                await using var enumerator = (await slot.StartReplication(idInfo.XLogPos)).GetAsyncEnumerator();

                // We need to start a separate thread here as the insert command wil not complete until
                // the transaction successfully completes (which we block here from the standby side) and by that
                // will occupy the connection it is bound to.
                var insertTask = Task.Run(async () =>
                {
                    await using var insertConn = await OpenConnectionAsync(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=on"
                    });
                    await insertConn.ExecuteNonQueryAsync($"INSERT INTO \"{tableName}\" (name) VALUES ('val1')");
                });

                // Begin Transaction, Insert, Commit Transaction
                for (var i = 0; i < 3; i++)
                {
                    Console.WriteLine($"Reading XLOG message {i+1}");
                    Assert.That(await enumerator.MoveNextAsync(), Is.True);
                    Console.WriteLine($"Read {enumerator.Current.WalStart} {enumerator.Current.WalEnd} {enumerator.Current.ServerClock}");
                }

                var result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we didn't report fsync yet

                // Report last received LSN
                await replConn.UpdateStatus();

                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we still didn't report fsync yet

                // Report last applied LSN
                await replConn.UpdateStatus(lastAppliedLsn: enumerator.Current.WalEnd);

                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we still didn't report fsync yet

                // Report last flushed LSN
                await replConn.UpdateStatus(lastAppliedLsn: enumerator.Current.WalEnd, lastFlushedLsn: enumerator.Current.WalEnd);

                await insertTask;
                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Now it's committed because we reported fsync

                insertTask = Task.Run(async () =>
                {
                    await using var insertConn = OpenConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=remote_apply"
                    });
                    await insertConn.ExecuteNonQueryAsync($"INSERT INTO \"{tableName}\" (name) VALUES ('val2')");
                });

                // Begin Transaction, Insert, Commit Transaction
                for (var i = 0; i < 3; i++)
                {
                    Console.WriteLine($"Reading XLOG message {i+1}");
                    Assert.That(await enumerator.MoveNextAsync(), Is.True);
                    Console.WriteLine($"Read {enumerator.Current.WalStart} {enumerator.Current.WalEnd} {enumerator.Current.ServerClock}");
                }

                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Not committed yet because we didn't report apply yet

                // Report last received LSN
                await replConn.UpdateStatus();

                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Not committed yet because we still didn't report apply yet

                // Report last applied LSN
                await replConn.UpdateStatus(lastAppliedLsn: enumerator.Current.WalEnd);

                await insertTask;
                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val2")); // Now it's committed because we reported apply

                insertTask = Task.Run(async () =>
                {
                    await using var insertConn = OpenConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=remote_write"
                    });
                    await insertConn.ExecuteNonQueryAsync($"INSERT INTO \"{tableName}\" (name) VALUES ('val3')");
                });

                // Begin Transaction, Insert, Commit Transaction
                for (var i = 0; i < 3; i++)
                {
                    Console.WriteLine($"Reading XLOG message {i+1}");
                    Assert.That(await enumerator.MoveNextAsync(), Is.True);
                    Console.WriteLine($"Read {enumerator.Current.WalStart} {enumerator.Current.WalEnd} {enumerator.Current.ServerClock}");
                }

                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val2")); // Not committed yet because we didn't report receive yet

                // Report last received LSN
                await replConn.UpdateStatus();

                await insertTask;
                result = await conn.ExecuteScalarAsync($"SELECT name FROM \"{tableName}\" ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val3")); // Now it's committed because we reported receive
            });


        [Test]
        public async Task IdentifySystem()
        {
            await using var conn = await OpenReplicationConnectionAsync();
            var identificationInfo = await conn.IdentifySystem();
            Assert.That(identificationInfo.XLogPos, Is.Not.EqualTo(default(LogSequenceNumber)));
        }

        [Test]
        public async Task Show()
        {
            await using var conn = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(conn, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");
            await using var replConn = await OpenReplicationConnectionAsync();
            Assert.That(await replConn.Show("integer_datetimes"), Is.EqualTo("on"));
        }

        [Test]
        public async Task CreateDropSlot()
        {
            await using var conn = await OpenReplicationConnectionAsync();
            var slot = await CreateReplicationSlot(conn, nameof(CreateDropSlot));
            await conn.DropReplicationSlot(slot.SlotName);
        }

        [Test(Description =
             "Tests whether an attempt to create a temporary replication slot gives a useful exception for PostgreSQL 9.6 and below."),
         NonParallelizable]
        public async Task CreateTemporarySlotThrowsForOldBackends()
        {
            await using var conn = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
            TestUtil.MaximumPgVersionExclusive(conn, "10.0",
                "Temporary replication slots are supported as of PostgreSQL 10");

            await using var replConn = await OpenReplicationConnectionAsync();
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                switch (replConn)
                {
                case NpgsqlLogicalReplicationConnection logicalReplicationConnection:
                    await logicalReplicationConnection.CreateReplicationSlot(
                        nameof(CreateTemporarySlotThrowsForOldBackends) + "_Logical",
                        "test_decoding",
                        true);
                    break;
                case NpgsqlPhysicalReplicationConnection physicalReplicationConnection:
                    await physicalReplicationConnection.CreateReplicationSlot(
                        nameof(CreateTemporarySlotThrowsForOldBackends) + "_Physical",
                        true);
                    break;
                }
            });
            Assert.That(ex.Message, Does.StartWith("Temporary replication slots were introduced in PostgreSQL 10."));
            Assert.That(ex.InnerException, Is.TypeOf<PostgresException>());
            if (ex.InnerException is PostgresException inner)
            {
                Assert.That(inner.SqlState, Is.EqualTo("42601"));
            }
        }

        #region Support

        static TimeSpan ParseTimespan(string str)
        {
            var span = str.AsSpan();
            var pos = 0;
            var number = 0;
            while (pos < span.Length)
            {
                var c = span[pos];
                if (!char.IsDigit(c))
                    break;
                number = number * 10 + (c - 0x30);
                pos++;
            }

            if (number == 0)
                return Timeout.InfiniteTimeSpan;
            if ("ms".AsSpan().Equals(span.Slice(pos), StringComparison.Ordinal))
                return TimeSpan.FromMilliseconds(number);
            if ("s".AsSpan().Equals(span.Slice(pos), StringComparison.Ordinal))
                return TimeSpan.FromSeconds(number);
            if ("min".AsSpan().Equals(span.Slice(pos), StringComparison.Ordinal))
                return TimeSpan.FromMinutes(number);
            if ("h".AsSpan().Equals(span.Slice(pos), StringComparison.Ordinal))
                return TimeSpan.FromHours(number);
            if ("d".AsSpan().Equals(span.Slice(pos), StringComparison.Ordinal))
                return TimeSpan.FromDays(number);

            throw new ArgumentException($"Can not parse timestamp '{span.ToString()}'");
        }

        #endregion Support
    }
}
