using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using NUnit.Framework;

namespace Npgsql.Tests
{
    public class ReplicationTests : TestBase
    {
        [Test, NonParallelizable]
        public async Task EndToEnd()
        {
            await using var conn = OpenConnection();
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            var slotName = nameof(EndToEnd);

            await conn.ExecuteNonQueryAsync("DROP TABLE IF EXISTS end_to_end_replication");
            await conn.ExecuteNonQueryAsync("CREATE TABLE end_to_end_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT)");

            await replConn.OpenAsync();
            var slotInfo = await replConn.CreateReplicationSlot(slotName, "test_decoding");

            var confirmedFlushLsn = await conn.ExecuteScalarAsync($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            Assert.That(confirmedFlushLsn, Is.Null);
            //Assert.That((await replConn.IdentifySystem()).XLogPos, Is.EqualTo(confirmedFlushLsn));

            // Make some changes
            await conn.ExecuteNonQueryAsync("INSERT INTO end_to_end_replication (name) VALUES ('val1')");
            await conn.ExecuteNonQueryAsync("UPDATE end_to_end_replication SET name='val2' WHERE name='val1'");

            var enumerator = replConn.StartReplicationRaw(slotInfo.SlotName, slotInfo.ConsistentPoint).GetAsyncEnumerator();

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data), Does.StartWith("BEGIN "));

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data),
                Is.EqualTo("table public.end_to_end_replication: INSERT: id[integer]:1 name[text]:'val1'"));

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data), Does.StartWith("COMMIT "));

            // Pretend we've completely processed this transaction, inform the server manually
            // (in real life we can wait until the automatic periodic update does this)
            //await replConn.SendStatusUpdate(msg.WalEnd, msg.WalEnd, msg.WalEnd);
            //confirmedFlushLsn = conn.ExecuteScalar($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            //Assert.That(confirmedFlushLsn, Is.Not.Null);  // There's obviously a misunderstanding here

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data), Does.StartWith("BEGIN "));

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data),
                Is.EqualTo("table public.end_to_end_replication: UPDATE: id[integer]:1 name[text]:'val2'"));

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(await ReadAllAsString(enumerator.Current.Data), Does.StartWith("COMMIT "));

            replConn.Cancel();

            // TODO: Bad example: pretend we don't know what's coming
            // Drain any messages
            while (await enumerator.MoveNextAsync()) ;

            // Make sure the connection is back to idle state
            Assert.That(await replConn.Show("integer_datetimes"), Is.EqualTo("on"));

            // TODO: Do this in all cases with Defer
            await replConn.DropReplicationSlot(slotName);

            static async Task<string> ReadAllAsString(Stream stream)
            {
                var memoryStream = new MemoryStream();
                await stream.CopyToAsync(memoryStream);
                return UTF8Encoding.UTF8.GetString(memoryStream.ToArray());
            }
        }

        [Test, NonParallelizable]
        public async Task OutputPlugin()
        {
            await using var conn = OpenConnection();
            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            var slotName = nameof(OutputPlugin);

            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS default_publication;
DROP TABLE IF EXISTS logical_replication_identity_default, logical_replication_identity_index, logical_replication_identity_full;

CREATE TABLE logical_replication_identity_default (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
CREATE TABLE logical_replication_identity_index (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);
CREATE TABLE logical_replication_identity_full (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL);

CREATE UNIQUE INDEX idx_logical_replication_identity_index_name ON logical_replication_identity_index (name);
ALTER TABLE logical_replication_identity_index REPLICA IDENTITY USING INDEX idx_logical_replication_identity_index_name;
ALTER TABLE logical_replication_identity_full REPLICA IDENTITY FULL;

CREATE PUBLICATION default_publication FOR TABLE logical_replication_identity_default, logical_replication_identity_index, logical_replication_identity_full;
");

            await replConn.OpenAsync();
            var slot = await replConn.CreateReplicationSlot(slotName);

            var confirmedFlushLsn = await conn.ExecuteScalarAsync($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            Assert.That(confirmedFlushLsn, Is.Null);

            // Make some changes
            await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication_identity_default (name) VALUES ('val'), ('val2')");
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication_identity_default SET name='val1' WHERE name='val'");
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication_identity_default WHERE name='val2'");
            await conn.ExecuteNonQueryAsync("TRUNCATE TABLE logical_replication_identity_default RESTART IDENTITY CASCADE");

            await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication_identity_index (name) VALUES ('val')");
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication_identity_index SET name='val1' WHERE name='val'");
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication_identity_index WHERE name='val1'");

            await conn.ExecuteNonQueryAsync("INSERT INTO logical_replication_identity_full (name) VALUES ('val')");
            await conn.ExecuteNonQueryAsync("UPDATE logical_replication_identity_full SET name='val1' WHERE name='val'");
            await conn.ExecuteNonQueryAsync("DELETE FROM logical_replication_identity_full WHERE name='val1'");

            await using var enumerator = replConn.StartReplication(slot.SlotName, slot.ConsistentPoint, "default_publication").GetAsyncEnumerator();

            #region REPLICA IDENTITY DEFAULT

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            var relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication_identity_default"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Insert
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            var insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val"));

            // Insert
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("2"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val2"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<UpdateMessage>());
            var updateMsg = (UpdateMessage)enumerator.Current;
            Assert.That(updateMsg.OldRow, Is.Null);
            Assert.That(updateMsg.KeyRow, Is.Null);
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<DeleteMessage>());
            var deleteMsg = (DeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.OldRow, Is.Null);
            Assert.That(deleteMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.KeyRow[0].Value, Is.EqualTo("2"));
            Assert.That(deleteMsg.KeyRow[1].Value, Is.Null);

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication_identity_default"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Truncate
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<TruncateMessage>());
            var truncateMsg = (TruncateMessage)enumerator.Current;
            Assert.That(truncateMsg.Options, Is.EqualTo(3));
            Assert.That(truncateMsg.RelationIds.Count, Is.EqualTo(1));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            #endregion REPLICA IDENTITY DEFAULT

            #region REPLICA USING INDEX idx_logical_replication_identity_index_name

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication_identity_index"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Insert
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<UpdateMessage>());
            updateMsg = (UpdateMessage)enumerator.Current;
            Assert.That(updateMsg.OldRow, Is.Null);
            Assert.That(updateMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(updateMsg.KeyRow![0].Value, Is.Null);
            Assert.That(updateMsg.KeyRow![1].Value, Is.EqualTo("val"));
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<DeleteMessage>());
            deleteMsg = (DeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.KeyRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.KeyRow[0].Value, Is.Null);
            Assert.That(deleteMsg.KeyRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            #endregion REPLICA USING INDEX idx_logical_replication_identity_index_name

            #region REPLICA IDENTITY FULL

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Relation
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<RelationMessage>());
            relMsg = (RelationMessage)enumerator.Current;
            Assert.That(relMsg.Namespace, Is.EqualTo("public"));
            Assert.That(relMsg.RelationName, Is.EqualTo("logical_replication_identity_full"));
            Assert.That(relMsg.Columns.Count, Is.EqualTo(2));
            Assert.That(relMsg.Columns[0].ColumnName, Is.EqualTo("id"));
            Assert.That(relMsg.Columns[1].ColumnName, Is.EqualTo("name"));

            // Insert
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<InsertMessage>());
            insertMsg = (InsertMessage)enumerator.Current;
            Assert.That(insertMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(insertMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(insertMsg.NewRow[1].Value, Is.EqualTo("val"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Update
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<UpdateMessage>());
            updateMsg = (UpdateMessage)enumerator.Current;
            Assert.That(updateMsg.KeyRow, Is.Null);
            Assert.That(updateMsg.OldRow!.Count, Is.EqualTo(2));
            Assert.That(updateMsg.OldRow![0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.OldRow![1].Value, Is.EqualTo("val"));
            Assert.That(updateMsg.NewRow.Count, Is.EqualTo(2));
            Assert.That(updateMsg.NewRow[0].Value, Is.EqualTo("1"));
            Assert.That(updateMsg.NewRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            // Begin Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Delete
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<DeleteMessage>());
            deleteMsg = (DeleteMessage)enumerator.Current;
            Assert.That(deleteMsg.OldRow!.Count, Is.EqualTo(2));
            Assert.That(deleteMsg.OldRow[0].Value, Is.EqualTo("1"));
            Assert.That(deleteMsg.OldRow[1].Value, Is.EqualTo("val1"));

            // Commit Transaction
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<CommitMessage>());

            #endregion REPLICA IDENTITY FULL
        }

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
            await using var conn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.OpenAsync();
            Assert.That(await conn.Show("integer_datetimes"), Is.EqualTo("on"));
        }

        [Test]
        public async Task CreateDropLogicalSlot()
        {
            await using var conn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await conn.OpenAsync();
            await conn.CreateReplicationSlot(nameof(CreateDropLogicalSlot), "test_decoding");
            await conn.DropReplicationSlot(nameof(CreateDropLogicalSlot));
        }

        #region Support

        [SetUp]
        public async Task Setup()
        {
            await using var conn = OpenConnection();
            var walLevel = (string)await conn.ExecuteScalarAsync("SHOW wal_level");
            if (walLevel != "logical")
                TestUtil.IgnoreExceptOnBuildServer("wal_level needs to be set to 'logical' in the PostgreSQL conf");
            await DropAllReplicationSlots();
        }

        async Task DropAllReplicationSlots()
        {
            await using var conn = OpenConnection();

            var slots = new List<string>();
            using (var cmd = new NpgsqlCommand("SELECT slot_name FROM pg_replication_slots", conn))
            await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                    slots.Add(reader.GetString(0));

            foreach (var slot in slots)
                await conn.ExecuteNonQueryAsync($"SELECT pg_drop_replication_slot('{slot}')");

        }

        #endregion Support
    }
}
