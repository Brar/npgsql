using System;
using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class TestDecodingLogicalReplicationTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {

        // The tests in this region are meant to run on PostgreSQL versions back to 9.4 where the
        // implementation of logical replication was still somewhat incomplete.
        // Please don't change them without confirming that they still work on those old versions.
        [Test(Description = "Tests whether INSERT commands get replicated via test_decoding plugin")]
        public Task StartReplicationStreamReplicatesInsert() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesInsert), async (slotName,tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"INSERT INTO \"{tableName}\" (name) VALUES ('val1'), ('val2')");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Insert first value
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": INSERT: id[integer]:1 name[text]:'val1'"));

                // Insert second value
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": INSERT: id[integer]:2 name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using the default replica identity")]
        public Task StartReplicationStreamReplicatesUpdateForDefaultReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesUpdateForDefaultReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"UPDATE \"{tableName}\" SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": UPDATE: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using an index as replica identity")]
        public Task StartReplicationStreamReplicatesUpdateForIndexReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesUpdateForIndexReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);

                var indexName = $"i_{tableName.Substring(2)}";
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX ""{indexName}"" ON ""{tableName}"" (name);
ALTER TABLE ""{tableName}"" REPLICA IDENTITY USING INDEX ""{indexName}"";
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"UPDATE \"{tableName}\" SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": UPDATE: old-key: name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using full replica identity")]
        public Task StartReplicationStreamReplicatesUpdateForFullReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesUpdateForFullReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE ""{tableName}"" REPLICA IDENTITY FULL;
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"UPDATE \"{tableName}\" SET name='val1' WHERE name='val'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Update
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": UPDATE: old-key: id[integer]:1 name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using the default replica identity")]
        public Task StartReplicationStreamReplicatesDeleteForDefaultReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesDeleteForDefaultReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"DELETE FROM \"{tableName}\" WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": DELETE: id[integer]:2"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using an index as replica identity")]
        public Task StartReplicationStreamReplicatesDeleteForIndexReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesDeleteForIndexReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = await OpenConnectionAsync();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                var indexName = $"i_{tableName.Substring(2)}";
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX ""{indexName}"" ON ""{tableName}"" (name);
ALTER TABLE ""{tableName}"" REPLICA IDENTITY USING INDEX ""{indexName}"";
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"DELETE FROM \"{tableName}\" WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": DELETE: name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using full replica identity")]
        public Task StartReplicationStreamReplicatesDeleteForFullReplicaIdentity() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesDeleteForFullReplicaIdentity), async (slotName, tableName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE ""{tableName}"" REPLICA IDENTITY FULL;
INSERT INTO ""{tableName}"" (name) VALUES ('val'), ('val2');
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"DELETE FROM \"{tableName}\" WHERE name='val2'");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Delete
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": DELETE: id[integer]:2 name[text]:'val2'"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });

        [Test(Description = "Tests whether TRUNCATE commands get replicated via test_decoding plugin")]
        public Task StartReplicationStreamReplicatesTruncate() =>
            SafeReplicationTest(nameof(StartReplicationStreamReplicatesInsert), async (slotName, tableName) =>
            {
                await using var conn = OpenConnection();
                TestUtil.MinimumPgVersion(conn, "11.0", "Replication of TRUNCATE commands was introduced in PostgreSQL 11");
                await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
                await conn.ExecuteNonQueryAsync(@$"
DROP TABLE IF EXISTS ""{tableName}"";
CREATE TABLE ""{tableName}"" (id serial PRIMARY KEY, name TEXT NOT NULL);
");
                await replConn.OpenAsync();
                var slot = await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
                    .CreateReplicationSlot(replConn, slotName);
                await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE \"{tableName}\" RESTART IDENTITY CASCADE");


                await using var enumerator = (await slot.StartReplication()).GetAsyncEnumerator();


                // Begin Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("BEGIN "));

                // Truncate
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data,
                    Is.EqualTo($"table public.\"{tableName}\": TRUNCATE: restart_seqs cascade"));

                // Commit Transaction
                Assert.That(await enumerator.MoveNextAsync(), Is.True);
                Assert.That(enumerator.Current.Data, Does.StartWith("COMMIT "));
            });
    }
}
