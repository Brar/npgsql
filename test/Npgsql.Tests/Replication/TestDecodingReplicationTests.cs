﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Logical.TestDecoding;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    /// <summary>
    /// These tests are meant to run on PostgreSQL versions back to 9.4 where the
    /// implementation of logical replication was still somewhat incomplete.
    /// Please don't change them without confirming that they still work on those old versions.
    /// </summary>
    public class TestDecodingReplicationTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {
        [Test]
        public Task CreateReplicationSlot()
            => SafeReplicationTest(nameof(CreateReplicationSlot) + "_test_decoding",
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

        [Test(Description = "Tests whether INSERT commands get replicated via test_decoding plugin")]
        public Task Insert()
            => SafeReplicationTest(nameof(Insert) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    await c.ExecuteNonQueryAsync($"CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL)");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"INSERT INTO {tableName} (name) VALUES ('val1'), ('val2')");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Insert first value
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: INSERT: id[integer]:1 name[text]:'val1'"));

                    // Insert second value
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: INSERT: id[integer]:2 name[text]:'val2'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    var exception = Assert.ThrowsAsync(Is.AssignableTo<OperationCanceledException>(), async () => await replicationTask);
                    if (c.PostgreSqlVersion < Version.Parse("9.4"))
                    {
                        Assert.That(exception, Has.InnerException.InstanceOf<PostgresException>()
                            .And.InnerException.Property(nameof(PostgresException.SqlState))
                            .EqualTo(PostgresErrorCodes.QueryCanceled));
                    }
                });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using the default replica identity")]
        public Task UpdateForDefaultReplicaIdentity()
            => SafeReplicationTest(nameof(UpdateForDefaultReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    await c.ExecuteNonQueryAsync($@"CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO {tableName} (name) VALUES ('val'), ('val2')");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"UPDATE {tableName} SET name='val1' WHERE name='val'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Update
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: UPDATE: id[integer]:1 name[text]:'val1'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    var exception = Assert.ThrowsAsync(Is.AssignableTo<OperationCanceledException>(), async () => await replicationTask);
                    if (c.PostgreSqlVersion < Version.Parse("9.4"))
                    {
                        Assert.That(exception, Has.InnerException.InstanceOf<PostgresException>()
                            .And.InnerException.Property(nameof(PostgresException.SqlState))
                            .EqualTo(PostgresErrorCodes.QueryCanceled));
                    }
                });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using an index as replica identity")]
        public Task UpdateForIndexReplicaIdentity()
            => SafeReplicationTest(nameof(UpdateForIndexReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    var indexName = $"i_{tableName.Substring(2)}";
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX {indexName} ON {tableName} (name);
ALTER TABLE {tableName} REPLICA IDENTITY USING INDEX {indexName};
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"UPDATE {tableName} SET name='val1' WHERE name='val'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Update
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: UPDATE: old-key: name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        [Test(Description = "Tests whether UPDATE commands get replicated via test_decoding plugin for tables using full replica identity")]
        public Task UpdateForFullReplicaIdentity()
            => SafeReplicationTest(nameof(UpdateForFullReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE {tableName} REPLICA IDENTITY FULL;
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"UPDATE {tableName} SET name='val1' WHERE name='val'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Update
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: UPDATE: old-key: id[integer]:1 name[text]:'val' new-tuple: id[integer]:1 name[text]:'val1'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using the default replica identity")]
        public Task DeleteForDefaultReplicaIdentity()
            => SafeReplicationTest(nameof(DeleteForDefaultReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"DELETE FROM {tableName} WHERE name='val2'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Delete
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: DELETE: id[integer]:2"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using an index as replica identity")]
        public Task DeleteForIndexReplicaIdentity()
            => SafeReplicationTest(nameof(DeleteForIndexReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    var indexName = $"i_{tableName.Substring(2)}";
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
CREATE UNIQUE INDEX {indexName} ON {tableName} (name);
ALTER TABLE {tableName} REPLICA IDENTITY USING INDEX {indexName};
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"DELETE FROM {tableName} WHERE name='val2'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Delete
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: DELETE: name[text]:'val2'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        [Test(Description = "Tests whether DELETE commands get replicated via test_decoding plugin for tables using full replica identity")]
        public Task DeleteForFullReplicaIdentity()
            => SafeReplicationTest(nameof(DeleteForFullReplicaIdentity) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    var indexName = $"i_{tableName.Substring(2)}";
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
ALTER TABLE {tableName} REPLICA IDENTITY FULL;
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"DELETE FROM {tableName} WHERE name='val2'");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Delete
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: DELETE: id[integer]:2 name[text]:'val2'"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });

        [Test(Description = "Tests whether TRUNCATE commands get replicated via test_decoding plugin")]
        public Task Truncate()
            => SafeReplicationTest(nameof(Truncate) + "_test_decoding",
                async (slotName, tableName) =>
                {
                    await using var c = await OpenConnectionAsync();
                    TestUtil.MinimumPgVersion(c, "11.0", "Replication of TRUNCATE commands was introduced in PostgreSQL 11");
                    var messages = new ConcurrentQueue<(NpgsqlLogSequenceNumber WalStart, NpgsqlLogSequenceNumber WalEnd, string Data)>();
                    var indexName = $"i_{tableName.Substring(2)}";
                    await c.ExecuteNonQueryAsync(@$"
CREATE TABLE {tableName} (id serial PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO {tableName} (name) VALUES ('val'), ('val2');
");
                    var rc = await OpenReplicationConnectionAsync();
                    var slot = await rc.CreateReplicationSlot(slotName);

                    await c.ExecuteNonQueryAsync($"TRUNCATE TABLE {tableName} RESTART IDENTITY CASCADE");

                    using var streamingCts = new CancellationTokenSource();
                    var replicationTask = Task.Run(async () =>
                    {
                        await foreach(var msg in (await rc.StartReplication(slot, cancellationToken: streamingCts.Token)).WithCancellation(streamingCts.Token))
                            messages.Enqueue((msg.WalStart, msg.WalEnd, msg.Data));
                    }, CancellationToken.None);

                    // Begin Transaction
                    var message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("BEGIN "));

                    // Truncate
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data,
                        Is.EqualTo($"table public.{tableName}: TRUNCATE: restart_seqs cascade"));

                    // Commit Transaction
                    message = await DequeueMessage(messages);
                    Assert.That(message.Data, Does.StartWith("COMMIT "));

                    streamingCts.Cancel();
                    Assert.That(async () => await replicationTask, Throws.Exception.AssignableTo<OperationCanceledException>()
                        .With.InnerException.InstanceOf<PostgresException>()
                        .And.InnerException.Property(nameof(PostgresException.SqlState))
                        .EqualTo(PostgresErrorCodes.QueryCanceled));
                    await rc.DropReplicationSlot(slotName, cancellationToken: CancellationToken.None);
                });
    }
}
