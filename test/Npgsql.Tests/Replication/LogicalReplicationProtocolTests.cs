using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.Protocol;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class LogicalReplicationProtocolTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
            var slot = await Npgsql.Replication.Logical.Protocol.NpgsqlLogicalReplicationConnectionExtensions
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
    }
}
