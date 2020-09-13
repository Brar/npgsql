using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Replication;
using Npgsql.Replication.Logical;
using Npgsql.Replication.Physical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    [TestFixture(typeof(NpgsqlLogicalReplicationConnection))]
    [TestFixture(typeof(NpgsqlPhysicalReplicationConnection))]
    public class CommonReplicationTests<TConnection> : SafeReplicationTestBase<TConnection>
        where TConnection : NpgsqlReplicationConnection, new()
    {
        #region Open

        [Test]
        public async Task Open()
        {
            await using var rc = await OpenReplicationConnectionAsync();
        }

        [Test]
        public void OpenCancelled()
            => Assert.That(async () =>
            {
                using var cts = GetCancelledCancellationTokenSource();
                await using var rc = await OpenReplicationConnectionAsync(cancellationToken: cts.Token);
            }, Throws.Exception.AssignableTo<OperationCanceledException>());

        [Test]
        public void OpenDisposed()
            => Assert.That(async () =>
            {
                var rc = await OpenReplicationConnectionAsync();
                await rc.DisposeAsync();
                await rc.OpenAsync();
            }, Throws.InstanceOf<ObjectDisposedException>()
                .With.Property(nameof(ObjectDisposedException.ObjectName))
                .EqualTo(typeof(TConnection).Name));

        #endregion Open

        #region IdentifySystem

        [Test]
        public async Task IdentifySystem()
        {
            await using var rc = await OpenReplicationConnectionAsync();
            var info = await rc.IdentifySystem();
            Assert.That(info.Timeline, Is.GreaterThan(0));
        }

        [Test]
        public void IdentifySystemCancelled()
            => Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                using var cts = GetCancelledCancellationTokenSource();
                await rc.IdentifySystem(cts.Token);
            }, Throws.Exception.AssignableTo<OperationCanceledException>());

        [Test]
        public void IdentifySystemDisposed()
            => Assert.That(async () =>
            {
                var rc = await OpenReplicationConnectionAsync();
                await rc.DisposeAsync();
                await rc.IdentifySystem();
            }, Throws.InstanceOf<ObjectDisposedException>()
                .With.Property(nameof(ObjectDisposedException.ObjectName))
                .EqualTo(typeof(TConnection).Name));

        #endregion IdentifySystem

        #region Show

        [Test]
        public async Task Show()
        {
            await using var c = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(c, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");

            await using var rc = await OpenReplicationConnectionAsync();
            Assert.That(await rc.Show("integer_datetimes"), Is.EqualTo("on"));
        }

        [Test]
        public async Task ShowNullArgument()
        {
            await using var c = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(c, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");

            Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                await rc.Show(null!);
            }, Throws.ArgumentNullException
                .With.Property("ParamName")
                .EqualTo("parameterName"));
        }

        [Test]
        public async Task ShowCancelled()
        {
            await using var c = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(c, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");

            Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                using var cts = GetCancelledCancellationTokenSource();
                await rc.Show("integer_datetimes", cts.Token);
            }, Throws.Exception.AssignableTo<OperationCanceledException>());
        }

        [Test]
        public async Task ShowDisposed()
        {
            await using var c = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(c, "10.0", "The SHOW command was added to the Streaming Replication Protocol in PostgreSQL 10");

            Assert.That(async () =>
            {
                var rc = await OpenReplicationConnectionAsync();
                await rc.DisposeAsync();
                await rc.Show("integer_datetimes");
            }, Throws.InstanceOf<ObjectDisposedException>()
                .With.Property(nameof(ObjectDisposedException.ObjectName))
                .EqualTo(typeof(TConnection).Name));
        }

        #endregion Show

        #region TimelineHistory

        [Test, Explicit("After initdb a PostgreSQL cluster only has one timeline and no timeline history so this command fails. " +
                        "You need to explicitly create multiple timelines (e. g. via PITR or by promoting a standby) for this test to work.")]
        public async Task TimelineHistory()
        {
            await using var rc = await OpenReplicationConnectionAsync();
            var systemInfo = await rc.IdentifySystem();
            var info = await rc.TimelineHistory(systemInfo.Timeline);
            Assert.That(info.FileName, Is.Not.Null);
            Assert.That(info.Content.Length, Is.GreaterThan(0));
            var contentText = Encoding.UTF8.GetString(info.Content);
        }

        [Test]
        public void TimelineHistoryCancelled()
            => Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                var systemInfo = await rc.IdentifySystem();
                using var cts = GetCancelledCancellationTokenSource();
                await rc.TimelineHistory(systemInfo.Timeline, cts.Token);
            }, Throws.Exception.AssignableTo<OperationCanceledException>());

        [Test]
        public void TimelineHistoryNonExisting()
            => Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                await rc.TimelineHistory(uint.MaxValue);
            }, Throws.InstanceOf<PostgresException>()
                .With.Property(nameof(PostgresException.SqlState))
                .EqualTo("58P01"));

        [Test]
        public void TimelineHistoryDisposed()
            => Assert.That(async () =>
            {
                var rc = await OpenReplicationConnectionAsync();
                var systemInfo = await rc.IdentifySystem();
                await rc.DisposeAsync();
                await rc.TimelineHistory(systemInfo.Timeline);
            }, Throws.InstanceOf<ObjectDisposedException>()
                .With.Property(nameof(ObjectDisposedException.ObjectName))
                .EqualTo(typeof(TConnection).Name));

        #endregion TimelineHistory

        #region DropReplicationSlot

        [Test]
        public void DropReplicationSlotNullSlot()
            => Assert.That(async () =>
            {
                await using var rc = await OpenReplicationConnectionAsync();
                await rc.DropReplicationSlot(null!);
            }, Throws.ArgumentNullException
                .With.Property("ParamName")
                .EqualTo("slotName"));

        [Test]
        public Task DropReplicationSlotCancelled()
            => SafeReplicationTest(nameof(DropReplicationSlotCancelled),
                async (slotName, _) =>
                {
                    await CreateReplicationSlot(slotName);
                    await using var rc = await OpenReplicationConnectionAsync();
                    using var cts = GetCancelledCancellationTokenSource();
                    Assert.That(async () => await rc.DropReplicationSlot(slotName, cancellationToken: cts.Token), Throws.Exception.AssignableTo<OperationCanceledException>());
                });

        [Test]
        public Task DropReplicationSlotDisposed()
            => SafeReplicationTest(nameof(DropReplicationSlotDisposed),
                async (slotName, _) =>
                {
                    await CreateReplicationSlot(slotName);
                    await using var rc = await OpenReplicationConnectionAsync();
                    await rc.DisposeAsync();
                    Assert.That(async () => await rc.DropReplicationSlot(slotName), Throws.InstanceOf<ObjectDisposedException>()
                        .With.Property(nameof(ObjectDisposedException.ObjectName))
                        .EqualTo(typeof(TConnection).Name));
                });

        #endregion

        #region BaseBackup

        // ToDo: Implement BaseBackup and create tests for it

        #endregion

        async Task CreateReplicationSlot(string slotName)
        {
            await using var c = await OpenConnectionAsync();
            await c.ExecuteNonQueryAsync(typeof(TConnection) == typeof(NpgsqlPhysicalReplicationConnection)
                ? $"SELECT pg_create_physical_replication_slot('{slotName}')"
                : $"SELECT pg_create_logical_replication_slot ('{slotName}', 'test_decoding')");
        }
    }
}
