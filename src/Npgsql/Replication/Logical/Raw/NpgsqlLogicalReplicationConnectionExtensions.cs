using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Replication.Internal;

namespace Npgsql.Replication.Logical.Raw
{
    /// <summary>
    /// Extension methods to use <see cref="NpgsqlLogicalReplicationConnection"/> with the
    /// <see cref="NpgsqlRawLogicalReplicationSlot"/> class.
    /// </summary>
    [PublicAPI]
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        /// <summary>
        /// Creates a <see cref="NpgsqlRawLogicalReplicationSlot"/> that wraps a replication slot using an arbitrary
        /// logical decoding plugin and can be used to start streaming replication using raw <see cref="XLogData"/>
        /// messages.
        /// </summary>
        /// <param name="connection">The <see cref="NpgsqlLogicalReplicationConnection"/> to use for creating the
        /// replication slot</param>
        /// <param name="slotName">The name of the slot to create.</param>
        /// <param name="outputPlugin">The name of the output plugin used for logical decoding.</param>
        /// <param name="temporary"><see langword="true"/> if this replication slot shall be temporary one; otherwise
        /// <see langword="false"/>. Temporary slots are not saved to disk and are automatically dropped on error or
        /// when the session has finished.</param>
        /// <param name="slotSnapshotInitMode">A <see cref="SlotSnapshotInitMode"/> to specify what to do with the
        /// snapshot created during logical slot initialization. <see cref="SlotSnapshotInitMode.Export"/>, which is
        /// also the default, will export the snapshot for use in other sessions. This option can't be used inside a
        /// transaction. <see cref="SlotSnapshotInitMode.Use"/> will use the snapshot for the current transaction
        /// executing the command. This option must be used in a transaction, and <see cref="SlotSnapshotInitMode.Use"/>
        /// must be the first command run in that transaction. Finally, <see cref="SlotSnapshotInitMode.NoExport"/> will
        /// just use the snapshot for logical decoding as normal but won't do anything else with it.</param>
        /// <returns>A <see cref="NpgsqlRawLogicalReplicationSlot"/> that wraps the newly-created replication slot.
        /// </returns>
        [PublicAPI]
        public static async Task<NpgsqlRawLogicalReplicationSlot> CreateReplicationSlot(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            string outputPlugin,
            bool temporary = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null)
        {
            var slotInfo =
                await connection.CreateReplicationSlotForPlugin(slotName, outputPlugin, temporary, slotSnapshotInitMode);

            return new NpgsqlRawLogicalReplicationSlot(connection, slotInfo.SlotName, slotInfo.ConsistentPoint,
                slotInfo.SnapshotName, slotInfo.OutputPlugin);
        }
    }
}
