using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
            await replConn.CreateReplicationSlot(slotName, "test_decoding");

            var confirmedFlushLsn = await conn.ExecuteScalarAsync($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            Assert.That(confirmedFlushLsn, Is.Null);
            //Assert.That((await replConn.IdentifySystem()).XLogPos, Is.EqualTo(confirmedFlushLsn));

            // Make some changes
            await conn.ExecuteNonQueryAsync("INSERT INTO end_to_end_replication (name) VALUES ('val1')");
            await conn.ExecuteNonQueryAsync("UPDATE end_to_end_replication SET name='val2' WHERE name='val1'");

            var enumerator = replConn.StartReplication(slotName, "0/0").GetAsyncEnumerator();

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
            var slotName = nameof(EndToEnd);

            await conn.ExecuteNonQueryAsync(@"
DROP PUBLICATION IF EXISTS foo_publication;
DROP TABLE IF EXISTS end_to_end_replication;
CREATE TABLE end_to_end_replication (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, name TEXT);
CREATE PUBLICATION foo_publication FOR TABLE end_to_end_replication;");

            await replConn.OpenAsync();
            await replConn.CreateOutputReplicationSlot(slotName);

            var confirmedFlushLsn = await conn.ExecuteScalarAsync($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            Assert.That(confirmedFlushLsn, Is.Null);
            //Assert.That((await replConn.IdentifySystem()).XLogPos, Is.EqualTo(confirmedFlushLsn));

            // Make some changes
            await conn.ExecuteNonQueryAsync("INSERT INTO end_to_end_replication (name) VALUES ('val1')");
            await conn.ExecuteNonQueryAsync("UPDATE end_to_end_replication SET name='val2' WHERE name='val1'");

            var enumerator = replConn.StartOutputReplication(slotName, "0/0", "foo_publication").GetAsyncEnumerator();

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.TypeOf<BeginMessage>());

            // Assert.That(UTF8Encoding.UTF8.GetString(enumerator.Current.Data.ToArray()),
            //     Is.EqualTo("table public.end_to_end_replication: INSERT: id[integer]:1 name[text]:'val1'"));
            //
            // Assert.That(await enumerator.MoveNextAsync(), Is.True);
            // Assert.That(UTF8Encoding.UTF8.GetString(enumerator.Current.Data.ToArray()), Does.StartWith("COMMIT "));
            //
            // // Pretend we've completely processed this transaction, inform the server manually
            // // (in real life we can wait until the automatic periodic update does this)
            // //await replConn.SendStatusUpdate(msg.WalEnd, msg.WalEnd, msg.WalEnd);
            // //confirmedFlushLsn = conn.ExecuteScalar($"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{slotName}'");
            // //Assert.That(confirmedFlushLsn, Is.Not.Null);  // There's obviously a misunderstanding here
            //
            // Assert.That(await enumerator.MoveNextAsync(), Is.True);
            // Assert.That(UTF8Encoding.UTF8.GetString(enumerator.Current.Data.ToArray()), Does.StartWith("BEGIN "));
            //
            // Assert.That(await enumerator.MoveNextAsync(), Is.True);
            // Assert.That(UTF8Encoding.UTF8.GetString(enumerator.Current.Data.ToArray()),
            //     Is.EqualTo("table public.end_to_end_replication: UPDATE: id[integer]:1 name[text]:'val2'"));
            //
            // Assert.That(await enumerator.MoveNextAsync(), Is.True);
            // Assert.That(UTF8Encoding.UTF8.GetString(enumerator.Current.Data.ToArray()), Does.StartWith("COMMIT "));
            //
            // replConn.Cancel();
            //
            // // TODO: Bad example: pretend we don't know what's coming
            // // Drain any messages
            //while (await enumerator.MoveNextAsync()) ;
            //
            // // Make sure the connection is back to idle state
            // Assert.That(await replConn.Show("integer_datetimes"), Is.EqualTo("on"));
            //
            // await replConn.DropReplicationSlot(slotName);
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
