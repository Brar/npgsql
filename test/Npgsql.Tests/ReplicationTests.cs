using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using NUnit.Framework;
using Npgsql.Replication.Logical.Protocol;

namespace Npgsql.Tests
{
    public class ReplicationTests : TestBase
    {

        [Test(Description = "Tests whether INSERT commands get replicated via test_decoding plugin"), NonParallelizable]
        public Task ReplicationSurvivesPausesLongerThanWalSenderTimeout() =>
            SafeTest(nameof(ReplicationSurvivesPausesLongerThanWalSenderTimeout), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "10.0", "The SHOW command, which is required to run this test was added to the Streaming Replication Protocol in PostgreSQL 10");
                await using var replConn = new NpgsqlLogicalReplicationConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    ApplicationName = slotName
                }.ToString());

                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await replConn.OpenAsync();
                var walSenderTimeout = ParseTimespan(await replConn.Show("wal_sender_timeout"));
                Console.WriteLine($"The server wal_sender_timeout is configured to {walSenderTimeout}");
                var walReceiverStatusInterval = TimeSpan.FromTicks(walSenderTimeout.Ticks / 2L);
                Console.WriteLine($"Setting {nameof(NpgsqlLogicalReplicationConnection)}.{nameof(NpgsqlLogicalReplicationConnection.WalReceiverStatusInterval)} to {walReceiverStatusInterval}");
                replConn.WalReceiverStatusInterval = walReceiverStatusInterval;
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions.CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val1')");
                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();

                var delay = TimeSpan.FromTicks(checked(walSenderTimeout.Ticks * 2L));
                Console.WriteLine($"Going to sleep for {delay}");
                await Task.Delay(delay);

                // Begin Transaction, Insert, Commit Transaction
                for (var i = 0; i < 3; i++)
                {
                    Assert.That(await enumerator.MoveNextAsync(), Is.True);
                }
            });


        [Test(Description = "Tests whether INSERT commands get replicated via test_decoding plugin"), NonParallelizable]
        public Task SynchronousReplication() =>
            SafeTest(nameof(SynchronousReplication), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                {
                    // This must be one of the configured synchronous_standby_names from postgresql.conf
                    ApplicationName = "npgsql_test_sync_standby"
                }.ToString());
                await replConn.OpenAsync();
                // Set WalReceiverStatusInterval to infinite so that the automated feedback doesn't interfere with
                // our manual feedback
                replConn.WalReceiverStatusInterval = Timeout.InfiniteTimeSpan;

                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
");

                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions.CreateReplicationSlot(replConn, slotName);
                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();

                // We need to start a separate thread here as the insert command wil not complete until
                // the transaction successfully completes (which we block here from the standby side) and by that
                // will occupy the connection it is bound to.
                var insertTask = Task.Run(async () =>
                {
                    await using var insertConn = OpenConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=on"
                    });
                    await insertConn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val1')");
                });
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: INSERT: id[integer]:1 name[text]:'val1'"));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));

                var result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we didn't report fsync yet

                // Report last received LSN
                await replConn.SendStatusUpdate(force: true);

                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we still didn't report fsync yet

                // Report last applied LSN
                await replConn.SendStatusUpdate(lastAppliedLsn: enumerator.Current.WalEnd, force: true);

                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.Null); // Not committed yet because we still didn't report fsync yet

                // Report last flushed LSN
                await replConn.SendStatusUpdate(lastAppliedLsn: enumerator.Current.WalEnd, lastFlushedLsn: enumerator.Current.WalEnd, force: true);

                await insertTask;
                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Now it's committed because we reported fsync

                insertTask = Task.Run(async () =>
                {
                    await using var insertConn = OpenConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=remote_apply"
                    });
                    await insertConn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val2')");
                });
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: INSERT: id[integer]:2 name[text]:'val2'"));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));

                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Not committed yet because we didn't report apply yet

                // Report last received LSN
                await replConn.SendStatusUpdate(force: true);

                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val1")); // Not committed yet because we still didn't report apply yet

                // Report last applied LSN
                await replConn.SendStatusUpdate(lastAppliedLsn: enumerator.Current.WalEnd, force: true);

                await insertTask;
                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val2")); // Now it's committed because we reported apply

                insertTask = Task.Run(async () =>
                {
                    await using var insertConn = OpenConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
                    {
                        Options = "synchronous_commit=remote_write"
                    });
                    await insertConn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val3')");
                });
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: INSERT: id[integer]:3 name[text]:'val3'"));
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));

                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val2")); // Not committed yet because we didn't report receive yet

                // Report last received LSN
                await replConn.SendStatusUpdate(force: true);

                await insertTask;
                result = await conn.ExecuteScalarAsync("SELECT name FROM logical_replication ORDER BY id DESC LIMIT 1;");
                Assert.That(result, Is.EqualTo("val3")); // Now it's committed because we reported receive
            });

        #region Logical Replication

        // The tests in this region are meant to run on PostgreSQL versions back to 9.4 where the
        // implementation of logical replication was still somewhat incomplete.
        // Please don't change them without confirming that they still work on those old versions.
        #region test_decoding (consuming XlogData.Data as raw stream)

        [Test(Description = "Tests whether INSERT commands get replicated via test_decoding plugin"), NonParallelizable]
        public Task StartReplicationStreamReplicatesInsert() =>
            SafeTest(nameof(StartReplicationStreamReplicatesInsert), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val1'), ('val2')");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Insert first value
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: INSERT: id[integer]:1 name[text]:'val1'"));

                // Insert second value
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: INSERT: id[integer]:2 name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using the default replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesUpdateForDefaultReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesUpdateForDefaultReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: UPDATE: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using an index as replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesUpdateForIndexReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesUpdateForIndexReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX idx_logical_replication_name ON logical_replication (name);
ALTER TABLE logical_replication REPLICA IDENTITY USING INDEX idx_logical_replication_name;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: UPDATE: old-key: name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using full replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesUpdateForFullReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesUpdateForFullReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE logical_replication REPLICA IDENTITY FULL;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: UPDATE: old-key: id[integer]:1 name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using the default replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesDeleteForDefaultReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesDeleteForDefaultReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: DELETE: id[integer]:2"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using an index as replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesDeleteForIndexReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesDeleteForIndexReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX idx_logical_replication_name ON logical_replication (name);
ALTER TABLE logical_replication REPLICA IDENTITY USING INDEX idx_logical_replication_name;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: DELETE: name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using full replica identity"), NonParallelizable]
        public Task StartReplicationStreamReplicatesDeleteForFullReplicaIdentity() =>
            SafeTest(nameof(StartReplicationStreamReplicatesDeleteForFullReplicaIdentity), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE logical_replication REPLICA IDENTITY FULL;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: DELETE: id[integer]:2 name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether TRUNCATE commands get replicated via test_decoding plugin"), NonParallelizable]
        public Task StartReplicationStreamReplicatesTruncate() =>
            SafeTest(nameof(StartReplicationStreamReplicatesInsert), async (slotName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "11.0", "Replication of TRUNCATE commands was introduced in PostgreSQL 11");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@"
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await replConn.OpenAsync();
                var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync("TRUNCATE TABLE logical_replication RESTART IDENTITY CASCADE");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Truncate
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo("table public.logical_replication: TRUNCATE: restart_seqs cascade"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        #endregion test_decoding (consuming XlogData.Data as raw stream)

        #region pgoutput (consuming Logical Replication Protocol Messages)

        [Test(Description = "Tests whether INSERT commands get replicated as Logical Replication Protocol Messages"), NonParallelizable]
        public async Task StartReplicationReplicatesInsert()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesInsert), true);
            await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication (name) VALUES ('val1'), ('val2')");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('d'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Insert first value
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            var insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Insert second value
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("2"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val2"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether UPDATE commands get replicated as Logical Replication Protocol Messages for tables using the default replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesUpdateForDefaultReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesUpdateForDefaultReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('d'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<UpdateMessage>());
            var updateMsg = (UpdateMessage)enumerator.Current;
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether UPDATE commands get replicated as Logical Replication Protocol Messages for tables using an index as replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesUpdateForIndexReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
CREATE UNIQUE INDEX idx_logical_replication_name ON logical_replication (name);
ALTER TABLE logical_replication REPLICA IDENTITY USING INDEX idx_logical_replication_name;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesUpdateForIndexReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('i'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<IndexUpdateMessage>());
            var updateMsg = (IndexUpdateMessage)enumerator.Current;
            Assert.That(updateMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(updateMsg.KeyRow![0].Value, Is.Null);
            Assert.That(updateMsg.KeyRow![1].Value, Is.EqualTo("val"));
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether UPDATE commands get replicated as Logical Replication Protocol Messages for tables using full replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesUpdateForFullReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
ALTER TABLE logical_replication REPLICA IDENTITY FULL;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesUpdateForFullReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication SET name='val1' WHERE name='val'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('f'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<FullUpdateMessage>());
            var updateMsg = (FullUpdateMessage)enumerator.Current;
            Assert.That(updateMsg.OldRow!.Count, Is.EqualTo(2));
            Assert.That(updateMsg.OldRow![0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.OldRow![1].Value, Is.EqualTo("val"));
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether DELETE commands get replicated as Logical Replication Protocol Messages for tables using the default replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesDeleteForDefaultReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await NpgsqlLogicalReplicationConnectionExtensions.CreateReplicationSlot(replConn,
                nameof(StartReplicationReplicatesDeleteForDefaultReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('d'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<KeyDeleteMessage>());
            var deleteMsg = (KeyDeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.KeyRow[0].Value, Is.EqualTo("2"));
            Assert.That(deleteMsg.KeyRow[1].Value, Is.Null);

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether DELETE commands get replicated as Logical Replication Protocol Messages for tables using an index as replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesDeleteForIndexReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
CREATE UNIQUE INDEX idx_logical_replication_name ON logical_replication (name);
ALTER TABLE logical_replication REPLICA IDENTITY USING INDEX idx_logical_replication_name;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesDeleteForIndexReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('i'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<KeyDeleteMessage>());
            var deleteMsg = (KeyDeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.KeyRow[0].Value, Is.Null);
            Assert.That(deleteMsg.KeyRow[1].Value, Is.EqualTo("val2"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether DELETE commands get replicated as Logical Replication Protocol Messages for tables using full replica identity"), NonParallelizable]
        public async Task StartReplicationReplicatesDeleteForFullReplicaIdentity()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The Logical Replication Protocol (via pgoutput plugin) was introduced in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
ALTER TABLE logical_replication REPLICA IDENTITY FULL;
INSERT INTO logical_replication (name) VALUES ('val'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesDeleteForFullReplicaIdentity), true);
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication WHERE name='val2'");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('f'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<FullDeleteMessage>());
            var deleteMsg = (FullDeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.OldRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.OldRow[0].Value, Is.EqualTo("2"));
            Assert.That(deleteMsg.OldRow[1].Value, Is.EqualTo("val2"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        [Test(Description = "Tests whether TRUNCATE commands get replicated as Logical Replication Protocol Messages on PostgreSQL 11 and above"), NonParallelizable]
        public async Task StartReplicationReplicatesTruncate()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "11.0", "Replication of TRUNCATE commands was introduced in PostgreSQL 11");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS npgsql_test_publication;
DROP TABLE IF EXISTS logical_replication;
CREATE TABLE logical_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
INSERT INTO logical_replication (name) VALUES ('val1'), ('val2');
CREATE PUBLICATION npgsql_test_publication FOR TABLE logical_replication;
");
            await replConn.OpenAsync();
            var slot = await Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(replConn, nameof(StartReplicationReplicatesTruncate), true);
            await conn.ExecuteNonQueryAsync("TRUNCATE TABLE logical_replication RESTART IDENTITY CASCADE");


            await using var enumerator = (await slot.StartReplication("npgsql_test_publication")).GetAsyncEnumerator();


            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.RelationReplicaIdentitySetting, Is.EqualTo('d'));
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Truncate
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<TruncateMessage>());
            var truncateMsg = (TruncateMessage)enumerator.Current;
            Assert.That(truncateMsg.Options, Is.EqualTo(TruncateOptions.Cascade | TruncateOptions.RestartIdentity));
            Assert.That(truncateMsg.RelationIds.Count, Is.EqualTo(1));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());
        }

        #endregion pgoutput (consuming Logical Replication Protocol Messages)

        #endregion Logical Replication

        #region Replication in general

        [Test]
        public async Task IdentifySystem()
        {
            await using var conn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.OpenAsync();
            var identificationInfo = await conn.IdentifySystem();
            Assert.That(identificationInfo.DbName, Is.EqualTo(new NpgsqlConnectionStringBuilder(ConnectionString).Database));
        }

        [Test]
        public async Task Show()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await replConn.OpenAsync();
            Assert.That(await replConn.Show("integer_datetimes"), Is.EqualTo("on"));
        }

        [Test]
        public async Task CreateDropLogicalSlot()
        {
            await using var conn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.OpenAsync();
            var slot = await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                .CreateReplicationSlot(conn, nameof(CreateDropLogicalSlot));
            await conn.DropReplicationSlot(slot.SlotName);
        }

        #endregion Replication in general

        #region Backwards compatibility

        [Test(Description = "Tests whether an attempt to create a temporary replication slot gives a useful exception for PostgreSQL 9.6 and below."), NonParallelizable]
        public async Task IsTemporaryThrowsForOldBackends()
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
            TestUtil.MaximumPgVersionExclusive(conn, "10.0", "Temporary replication slots are supported as of PostgreSQL 10");

            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await replConn.OpenAsync();
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, nameof(IsTemporaryThrowsForOldBackends), true);
            });
            Assert.That(ex.Message, Does.StartWith("Temporary replication slots were introduced in PostgreSQL 10."));
            Assert.That(ex.InnerException, Is.TypeOf<PostgresException>());
            if (ex.InnerException is PostgresException inner)
            {
                Assert.That(inner.SqlState, Is.EqualTo("42601"));
            }
        }

        [Test(Description = "Tests whether an attempt to create a temporary replication slot gives a useful exception for PostgreSQL 9.6 and below."), NonParallelizable]
        [TestCase(SlotSnapshotInitMode.Use)]
        [TestCase(SlotSnapshotInitMode.NoExport)]
        public async Task NonDefaultSlotSnapshotInitModeThrowsForOldBackends(SlotSnapshotInitMode mode)
        {
            await using var conn = OpenConnection();
            TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
            TestUtil.MaximumPgVersionExclusive(conn, "10.0", "The USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL 10");

            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await replConn.OpenAsync();
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                await Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, nameof(NonDefaultSlotSnapshotInitModeThrowsForOldBackends), slotSnapshotInitMode: mode);
            });
            Assert.That(ex.Message, Does.StartWith("The USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL 10."));
            Assert.That(ex.InnerException, Is.TypeOf<PostgresException>());
            if (ex.InnerException is PostgresException inner)
            {
                Assert.That(inner.SqlState, Is.EqualTo("42601"));
            }
        }

        #endregion Backwards compatibility

        #region Support

        [SetUp]
        public async Task Setup()
        {
            await using var conn = OpenConnection();
            var walLevel = (string)await conn.ExecuteScalarAsync("SHOW wal_level");
            if (walLevel != "logical")
                TestUtil.IgnoreExceptOnBuildServer("wal_level needs to be set to 'logical' in the PostgreSQL conf");
        }

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

        async Task SafeTest(string slotName, Func<string, Task> testAction)
        {
            try
            {
                await testAction(slotName);
            }
            finally
            {
                await using var replConn2 = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await replConn2.OpenAsync();
                try
                {
                    await replConn2.DropReplicationSlot(slotName);
                }
                catch (PostgresException e)
                {
                    // Temporary slots might already have been deleted
                    if (e.SqlState != "42704" || !e.Message.Contains(slotName.ToLowerInvariant()))
                    {
                        throw;
                    }
                }
            }
        }

        #endregion Support
    }
}
