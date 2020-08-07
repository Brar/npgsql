using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.Replication.Internal;

namespace Npgsql.Replication.Logical.TestDecoding
{
    /// <summary>
    /// Extension methods to use <see cref="NpgsqlLogicalReplicationConnection"/> with the
    /// <see cref="NpgsqlTestDecodingReplicationSlot"/> class.
    /// </summary>
    [PublicAPI]
    public static class NpgsqlLogicalReplicationConnectionExtensions
    {
        /// <summary>
        /// Creates a <see cref="NpgsqlTestDecodingReplicationSlot"/> that wraps a replication slot using the
        /// "test_decoding" logical decoding plugin and can be used to start streaming replication using
        /// <see cref="NpgsqlTestDecodingData"/> messages.
        /// </summary>
        /// <param name="connection">The <see cref="NpgsqlLogicalReplicationConnection"/> to use for creating the
        /// replication slot</param>
        /// <param name="slotName">The name of the slot to create.</param>
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
        /// <returns>A <see cref="NpgsqlTestDecodingReplicationSlot"/> that wraps the newly-created replication slot.
        /// </returns>
        [PublicAPI]
        public static async Task<NpgsqlTestDecodingReplicationSlot> CreateReplicationSlot(
            this NpgsqlLogicalReplicationConnection connection,
            string slotName,
            bool temporary = false,
            SlotSnapshotInitMode? slotSnapshotInitMode = null)
        {
            var slotInfo =
                await connection.CreateReplicationSlotForPlugin(slotName, "test_decoding", temporary, slotSnapshotInitMode);

            return new NpgsqlTestDecodingReplicationSlot(connection, slotInfo.SlotName, slotInfo.ConsistentPoint,
                slotInfo.SnapshotName!);
        }
    }
}
