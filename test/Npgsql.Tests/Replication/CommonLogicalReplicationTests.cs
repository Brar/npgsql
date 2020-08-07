using System;
using System.Threading.Tasks;
using Npgsql.Replication.Logical;
using NUnit.Framework;

namespace Npgsql.Tests.Replication
{
    public class CommonLogicalReplicationTests : SafeReplicationTestBase<NpgsqlLogicalReplicationConnection>
    {

        #region Backwards compatibility

        [Test(Description = "Tests whether an attempt to create a temporary replication slot gives a useful exception for PostgreSQL 9.6 and below."), NonParallelizable]
        [TestCase(SlotSnapshotInitMode.Use)]
        [TestCase(SlotSnapshotInitMode.NoExport)]
        public async Task NonDefaultSlotSnapshotInitModeThrowsForOldBackends(SlotSnapshotInitMode mode)
        {
            await using var conn = await OpenConnectionAsync();
            TestUtil.MinimumPgVersion(conn, "9.4", "Logical Replication was introduced in PostgreSQL 9.4");
            TestUtil.MaximumPgVersionExclusive(conn, "10.0", "The USE_SNAPSHOT and NOEXPORT_SNAPSHOT syntax was introduced in PostgreSQL 10");

            await using var replConn = new NpgsqlLogicalReplicationConnection(ConnectionString);
            await replConn.OpenAsync();
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                await Npgsql.Replication.Logical.TestDecoding.NpgsqlLogicalReplicationConnectionExtensions
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
    }
}
